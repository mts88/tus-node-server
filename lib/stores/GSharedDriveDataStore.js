'use strict';

const fs = require('fs');
const {google} = require('googleapis');
const {JWT} = require('google-auth-library');
const pkg = require('../../package.json');
const Configstore = require('configstore');
const FileStore = require('./FileStore');
const ERRORS = require('../constants').ERRORS;
const EVENTS = require('../constants').EVENTS;
const debug = require('debug');
const log = debug('tus-node-server:stores:gshareddrivestore');

const drive = google.drive('v3');

const File = require('../models/File');
const DEFAULT_CONFIG = {
    scopes: [
        'https://www.googleapis.com/auth/drive', 
        'https://www.googleapis.com/auth/admin.directory.group'
    ],
    drive_options: {
        corpora: 'drive',
        supportsAllDrives: true, // Mandatory for shared drive
        includeItemsFromAllDrives: true, // Mandatory for shared drive
    }
};


/**
 * @fileOverview
 * Store using Google Shared Drive filesystem.
 *
 * @author Francesca Motisi <fsca.motisi@gmail.com>
 */

class GSharedDriveDataStore extends FileStore {
    constructor(options) {
        super(options);
        this.extensions = ['creation', 'creation-defer-length'];

        if (!options.drive_id) {
            throw new Error('GSharedDriveDataStore must have a drive id');
        }

        this.drive_id = options.drive_id;

        if (!options.keyFilename) {
            throw new Error('GSharedDriveDataStore must have a keyFilename')
        }

        this.configstore = new Configstore(`${pkg.name}-${pkg.version}`);

        this.auth = this._loadSecret(options.keyFilename, options.subject);

        // Enable global authentication on googleapis
        google.options({
            auth: this.auth
        })

        this._checkDrive();
    }

    /**
     * Load credentials file
     * 
     * @param {string} path Path to file
     * @param {string} subject Subject to impersonate
     * 
     * @returns JWT Auth
     */
    _loadSecret(path, subject) {
        // Load client secrets from a local file.
        const content = fs.readFileSync(path);

        if ( !content ){
            throw new Error('Error loading client secret file')
        }
        
        return this._authorize(JSON.parse(content), subject);
    }

    /**
     * Authenticate with service account or credentials.json
     * 
     * @param {[type]} credentials Json parsed credentials file
     * @param {string} subject Subject to impersonate
     * 
     * @returns JWT Auth
     */
    _authorize(credentials, subject) {
        let jwtOptions = {
            email: credentials.client_email,
            key: credentials.private_key,
            keyId: credentials.private_key_id,
            scopes: DEFAULT_CONFIG.scopes
        };

        if(subject) {
            jwtOptions = {...jwtOptions, subject}
        }

        return new JWT(jwtOptions);
    }

    /**
     * Check if selected drive exists
     */
    async _checkDrive() {
        try {
            const res = await drive.drives.get({
                driveId: this.drive_id
            });
            log('[GSharedDriveStore] Founded Shared Drive with id ' + res.data.id + ' and name ' + res.data.name)
        } catch(err) {
            throw new Error(err)
        }
    }


    /**
     * Create an empty file.
     *
     * @param  {object} req http.incomingMessage
     * @param  {File} file
     * @return {Promise}
     */
    create(req) {
        return new Promise((resolve, reject) => {
            const upload_length = req.headers['upload-length'];
            const upload_defer_length = req.headers['upload-defer-length'];
            const upload_metadata = req.headers['upload-metadata'];

            if (upload_length === undefined && upload_defer_length === undefined) {
                return reject(ERRORS.INVALID_LENGTH);
            }

            let file_id;
            try {
                file_id = this.generateFileName(req);
            }
            catch (generateError) {
                log('[FileStore] create: check your namingFunction. Error', generateError);
                return reject(ERRORS.FILE_WRITE_ERROR);
            }

            const file = new File(file_id, upload_length, upload_defer_length, upload_metadata);

            return fs.open(`${this.directory}/${file.id}`, 'w', (err, fd) => {
                if (err) {
                    log('[FileStore] create: Error', err);
                    return reject(err);
                }

                this.configstore.set(file.id, file);

                return fs.close(fd, (exception) => {
                    if (exception) {
                        log('[FileStore] create: Error', exception);
                        return reject(exception);
                    }

                    this.emit(EVENTS.EVENT_FILE_CREATED, { file });
                    return resolve(file);
                });
            });
        });
    }

    /**
     * Write to the file, starting at the provided offset
     *
     * @param  {object} req http.incomingMessage
     * @param  {string} file_id   Name of file
     * @param  {integer} offset     starting offset
     * @return {Promise}
     */
    write(req, file_id, offset) {
        return new Promise(async (resolve, reject) => {
            const path = `${this.directory}/${file_id}`;
            const options = {
                flags: 'r+',
                start: offset,
            };

            const stream = fs.createWriteStream(path, options);

            let new_offset = 0;
            req.on('data', (buffer) => {
                new_offset += buffer.length;
            });

            stream.on('error', (e) => {
                log('[GSharedDriveStore] write: Error', e);
                reject(ERRORS.FILE_WRITE_ERROR);
            });
            const this$ = this;
            return req.pipe(stream).on('finish', () => {
                log(`[GSharedDriveStore] write: ${new_offset} bytes written to ${path}`);
                offset += new_offset;
                log(`[GSharedDriveStore] write: File is now ${offset} bytes`);

                const config = this.configstore.get(file_id);
                
                const metadata = this._parseMetadata(config.upload_metadata);

                if (config && parseInt(config.upload_length, 10) === offset) {

                    drive.files.create({
                        fields: 'id',
                        supportsAllDrives: true,
                        requestBody: {
                            mimeType: metadata.mimeType,
                            name: metadata.name, // to set filename
                            parents: [this$.drive_id], // to put in shared drive
                            // properties: {
                            //     'test': 'me'
                            // }
                        },
                        media: {
                            mimeType: metadata.mimeType,
                            body: fs.createReadStream(path)
                        }
                    }, function(err, res) {
                        if(err) {
                            log('[GSharedDriveStore] create: Error', err);
                            return err
                        }
                        fs.unlink(path, function(err) {
                            log('[GSharedDriveStore] Unlink localfile error ', err);
                        });
    
                        this$.configstore.delete(file_id);
                        this$.emit(EVENTS.EVENT_UPLOAD_ON_GDRIVE_COMPLETE, {file: {id: res.data.id, drive_id: this$.drive_id} })
                        this$.emit(EVENTS.EVENT_UPLOAD_COMPLETE, { file: { ...config, id: res.data.id, drive_id: this$.drive_id } });
                    });

                }
                resolve(offset);
            });
        });
    }

    _parseMetadata(metadata) {
        const parts = metadata.split(',')
        const meta = {};

        parts.map( p => {
            const nameValues = p.split(' ');
            const text = Buffer.from(nameValues[1], 'base64').toString('ascii');
            meta[nameValues[0]] = text;
        })

        return meta;
    }
}

module.exports = GSharedDriveDataStore;
