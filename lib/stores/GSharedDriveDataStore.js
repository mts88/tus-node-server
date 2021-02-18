'use strict';

const fs = require('fs');
const readline = require('readline');
const {google} = require('googleapis');
const {JWT, auth} = require('google-auth-library');

const DataStore = require('./DataStore');
const File = require('../models/File');
const { Storage } = require('@google-cloud/storage');
const stream = require('stream');
const ERRORS = require('../constants').ERRORS;
const EVENTS = require('../constants').EVENTS;
const TUS_RESUMABLE = require('../constants').TUS_RESUMABLE;
const DEFAULT_CONFIG = {
    scopes: ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/admin.directory.group'],
};

const debug = require('debug');
const log = debug('tus-node-server:stores:gshareddrivestore');

/**
 * @fileOverview
 * Store using Google Shared Drive filesystem.
 *
 * @author Francesca Motisi <fsca.motisi@gmail.com>
 */

class GSharedDriveDataStore extends DataStore {
    constructor(options) {
        super(options);
        this.extensions = ['creation', 'creation-defer-length'];

        if (!options.drive_name) {
            throw new Error('GSharedDriveDataStore must have a drive name');
        }

        this.drive_name = options.drive_name;

        if (!options.keyFilename) {
            throw new Error('GSharedDriveDataStore must have a keyFilename')
        }

        this.auth = this._loadSecret(options.keyFilename);
        // Enable global authentication on googleapis
        google.options({
            auth: this.auth
        })
        this.listFiles();
    }

    /**
     * Load credentials file
     * @param {credentials.json} path 
     */
    _loadSecret(path) {
        // Load client secrets from a local file.
        const content = fs.readFileSync(path);

        if ( !content ){
            throw new Error('Error loading client secret file')
        }
        
        return this._authorize(JSON.parse(content));
    }

    /**
     * Authenticate with service account or credentials.json
     * @param {credentials object} credentials Json parsed credentials file
     */
    _authorize(credentials) {
        return new JWT({
            email: credentials.client_email,
            key: credentials.private_key,
            keyId: credentials.private_key_id,
            scopes: DEFAULT_CONFIG.scopes,
            subject: "admin@cloudvision.space"
        });
    }
    
    listFiles() {
        const drive = google.drive({version: 'v3'});
        drive.files.list({
            driveId: "0AO0SylV0Dy96Uk9PVA",
            corpora: 'drive',
            supportsAllDrives: true, // Support for shared drive
            includeItemsFromAllDrives: true,
            pageSize: 10,
            fields: 'nextPageToken, files(id, name)',
        }, (err, res) => {
            if (err) return console.log('The API returned an error: ' + err);
            const files = res.data.files;
            if (files.length) {
                console.log('Files:');
                files.map((file) => {
                    console.log(`${file.name} (${file.id})`);
                });
            } else {
                console.log('No files found.');
            }
        });
    }

    /**
     * Check the bucket exists in GCS.
     *
     * @return {[type]} [description]
     */
    _getBucket() {
        const bucket = this.gcs.bucket(this.bucket_name);
        bucket.exists((error, exists) => {
            if (error) {
                log(error);
                throw new Error(`[GCSDataStore] _getBucket: ${error.message}`);
            }

            if (!exists) {
                throw new Error(`[GCSDataStore] _getBucket: ${this.bucket_name} bucket does not exist`);
            }

        });

        return bucket;
    }

    /**
     * Create an empty file in GCS to store the metatdata.
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
                reject(ERRORS.INVALID_LENGTH);
                return;
            }

            let file_id;
            try {
                file_id = this.generateFileName(req);
            }
            catch (generateError) {
                log('[FileStore] create: check your namingFunction. Error', generateError);
                reject(ERRORS.FILE_WRITE_ERROR);
                return;
            }

            const file = new File(file_id, upload_length, upload_defer_length, upload_metadata);
            const gcs_file = this.bucket.file(file.id);
            const options = {
                metadata: {
                    metadata: {
                        upload_length: file.upload_length,
                        tus_version: TUS_RESUMABLE,
                        upload_metadata,
                        upload_defer_length,
                    },
                },
            };

            const fake_stream = new stream.PassThrough();
            fake_stream.end();
            fake_stream.pipe(gcs_file.createWriteStream(options))
                .on('error', reject)
                .on('finish', () => {
                    this.emit(EVENTS.EVENT_FILE_CREATED, { file });
                    resolve(file);
                });
        });
    }

    /**
     * Get the file metatata from the object in GCS, then upload a new version
     * passing through the metadata to the new version.
     *
     * @param  {object} req         http.incomingMessage
     * @param  {string} file_id     Name of file
     * @param  {integer} offset     starting offset
     * @return {Promise}
     */
    write(req, file_id, offset) {
        // GCS Doesn't persist metadata within versions,
        // get that metadata first
        return this.getOffset(file_id)
            .then((data) => {
                return new Promise((resolve, reject) => {
                    const file = this.bucket.file(file_id);

                    const options = {
                        offset,
                        metadata: {
                            metadata: {
                                upload_length: data.upload_length,
                                tus_version: TUS_RESUMABLE,
                                upload_metadata: data.upload_metadata,
                                upload_defer_length: data.upload_defer_length,
                            },
                        },
                    };

                    const write_stream = file.createWriteStream(options);
                    if (!write_stream) {
                        return reject(ERRORS.FILE_WRITE_ERROR);
                    }

                    let new_offset = 0;
                    req.on('data', (buffer) => {
                        new_offset += buffer.length;
                    });

                    write_stream.on('finish', () => {
                        log(`${new_offset} bytes written`);

                        if (data.upload_length === new_offset) {
                            this.emit(EVENTS.EVENT_UPLOAD_COMPLETE, { file });
                        }

                        resolve(new_offset);
                    });

                    write_stream.on('error', (e) => {
                        log(e);
                        reject(ERRORS.FILE_WRITE_ERROR);
                    });

                    return req.pipe(write_stream);
                });
            });
    }

    /**
     * Get file metadata from the GCS Object.
     *
     * @param  {string} file_id     name of the file
     * @return {object}
     */
    getOffset(file_id) {
        return new Promise((resolve, reject) => {
            const file = this.bucket.file(file_id);
            file.getMetadata((error, metadata, apiResponse) => {
                if (error && error.code === 404) {
                    return reject(ERRORS.FILE_NOT_FOUND);
                }

                if (error) {
                    log('[GCSDataStore] getFileMetadata', error);
                    return reject(error);
                }

                const data = {
                    size: parseInt(metadata.size, 10),
                };

                if (!('metadata' in metadata)) {
                    return resolve(data);
                }

                if (metadata.metadata.upload_length) {
                    data.upload_length = parseInt(metadata.metadata.upload_length, 10);
                }

                if (metadata.metadata.upload_defer_length) {
                    data.upload_defer_length = parseInt(metadata.metadata.upload_defer_length, 10);
                }

                if (metadata.metadata.upload_metadata) {
                    data.upload_metadata = metadata.metadata.upload_metadata;
                }

                return resolve(data);
            });
        });
    }
}

module.exports = GSharedDriveDataStore;
