#!/usr/bin/env node

'use strict';

var path = require('path');
var semver = require('semver');
var request = require('request');
var express = require('express');
var level = require('level');
var crypto = require('crypto');
var mkdirp = require('mkdirp');
var proxy = require('express-http-proxy');
var compression = require('compression');
var favicon = require('serve-favicon');
var serveStatic = require('serve-static');
var then = require('then-levelup');

var Logger = require('./logger');
var util = require('./util');
var pkg = require('./../package.json');
var findVersion = require('./find-version');
var pouchServerLite = require('./pouchdb-server-lite');

module.exports = (options, callback) => {
    var FAT_REMOTE = options.remote;
    var SKIM_REMOTE = options.remoteSkim;
    var port = options.port;
    var pouchPort = options.pouchPort;
    var localBase = options.url.replace(/:5080$/, ':' + port); // port is configurable
    var directory = path.resolve(options.directory);
    var logger = new Logger(Logger.getLevel(options.logLevel));
    mkdirp.sync(directory);
    var startingTimeout = 1000;

    logger.code('Welcome!');
    logger.code('To start using local-npm, just run: ');
    logger.code(`   $ npm set registry ${localBase}`);
    logger.code('To switch back, you can run: ');
    logger.code(`   $ npm set registry ${FAT_REMOTE}`);

    var backoff = 1.1;
    var app = express();
    var PouchDB = pouchServerLite(options).PouchDB;

    var skimRemote = new PouchDB(SKIM_REMOTE);
    var skimLocal = new PouchDB('skimdb', {
        auto_compaction: true
    });
    var db = then(level(path.resolve(directory, 'binarydb')));

    logger.code('\nA simple npm-like UI is available here');
    logger.code(`http://127.0.0.1:${port}/_browse`);

    app.use(util.request(logger));
    app.use(compression());
    app.use(favicon(path.resolve(__dirname, '..', 'dist', 'favicon.ico')));
    app.use(serveStatic(path.resolve(__dirname, '..', 'dist')));
    app.use('/_browse', serveStatic(path.resolve(__dirname, '..', 'dist')));
    app.use('/_browse*', serveStatic(path.resolve(__dirname, '..', 'dist')));
    var allowCrossDomain = function(req,res,next){
        res.header("Access-Control-Allow-Origin","*");
        next();
    }
    app.use(allowCrossDomain)
    app.get('/_skimdb', redirectToSkimdb);
    app.get('/_skimdb*', redirectToSkimdb);
    app.get('/-/*', proxy(FAT_REMOTE, {
        limit: Infinity
    }));
    app.get('/', (req, res) => {
        Promise.all([skimLocal.info(), getCount()])
            .then((resp) => {
                res.json({
                    'local-npm': 'welcome',
                    version: pkg.version,
                    db: resp[0],
                    tarballs: resp[1]
                });
            });
    });


    //
    // utils
    //
    function redirectToSkimdb(req, res) {
        var skimUrl = 'http://localhost:' + pouchPort + '/skimdb';
        var get = request.get(req.originalUrl.replace(/^\/_skimdb/, skimUrl));
        get.on('error', (err) => {
            logger.warn("couldn't proxy to skimdb");
            logger.warn(err);
        });
        get.pipe(res);
    }

    function massageMetadata(urlBase, doc) {
        var name = doc.name;
        var versions = Object.keys(doc.versions);
        for (var i = 0, len = versions.length; i < len; i++) {
            var version = versions[i];
            if (!semver.valid(version)) {
                // apparently some npm modules like handlebars
                // have invalid semver ranges, and npm deletes them
                // on-the-fly
                delete doc.versions[version];
            } else {
                doc.versions[version].dist.tarball = urlBase + '/' + 'tarballs/' + name + '/' + version + '.tgz';
                doc.versions[version].dist.info = urlBase + '/' + name + '/' + version;
            }
        }
        return doc;

    }

    function sendBinary(res, buffer) {
        res.set('content-type', 'application/octet-stream');
        res.set('content-length', buffer.length);
        return res.send(buffer);
    }

    function cacheResponse(res, etag) {
        // do this to be more like registry.npmjs.com. not sure if it
        // actually has a benefit, though
        res.set('ETag', '"' + etag + '"');
        res.set('Cache-Control', 'max-age=300');
    }

    function getDocument(name) {
        // let's save the package data
        return skimLocal.get(name)
            .then((doc) => {
                console.log(name,"?????????doc")
                //???????????????doc??????????????????????????????????????????????????????
                return skimRemote.get(name)
                    .then((_doc) => {
                        let originRemoteDoc = JSON.parse(JSON.stringify(_doc));
                        if(_doc.time.modified >= doc.time.modified){
                            delete _doc['_rev'];
                            // ???????????????????????????versions
                            _doc.versions = JSON.parse(JSON.stringify(doc.versions));
                            _doc['dist-tags']['latest'] = findVersion.findLatest(_doc.versions)
                            return skimLocal.remove(doc)
                                .then(() => {
                                    return skimLocal.post(_doc)
                                        .then(() => {
                                            console.log(name,"???????????????????????????")
                                            return new Promise(function(resolve,reject){
                                                resolve(originRemoteDoc)
                                            })
                                        })
                                })
                        }else{
                            console.log(name,"??????????????????")
                            return new Promise(function(resolve,reject){
                                resolve(originRemoteDoc)
                            });
                        }
                    })
                    .catch((exception) => {
                        console.log("?????????doc????????????")
                        // ?????????????????????????????????????????????????????????
                        if(exception.code === 'ECONNREFUSED'){
                            console.log(name,"???????????????????????????")
                        }else if(exception.error === 'not_found'){
                            console.log(name,"???????????????????????????????????????")
                        }else if(exception.code === 'ETIMEDOUT'){
                            console.log("????????????????????????",exception)
                        }else{
                            console.log("????????????",exception)
                        }
                        return new Promise(function(resolve,reject){
                            resolve(doc)
                        });
                    })
            })
            .catch(() => {
                console.log(name,"????????????doc")
                return skimRemote.get(name)
                    .then((doc) => {
                        let originDoc = JSON.parse(JSON.stringify(doc))
                        delete doc['_rev'];
                        // versions?????????????????????????????????????????????????????????
                        let version = doc['dist-tags']['latest'];
                        let pkg = doc.versions[version]
                        doc.versions = new Object();
                        doc.versions[version] = pkg
                        doc.firstimport = true
                        skimLocal.post(doc)
                        return new Promise(function(resolve,reject){
                            resolve(originDoc)
                        })
                    })
                    .catch((exception) => {
                        console.log("?????????doc????????????")
                        if(exception.code === 'ECONNREFUSED'){
                            console.log(name,"??????????????????????????????????????????????????????????????????")
                        }else if(exception.error === 'not_found'){
                            console.log(name,"???????????????????????????????????????")
                        }else if(exception.code === 'ETIMEDOUT'){
                            console.log("????????????????????????",exception)
                        }else{
                            console.log("????????????",exception)
                        }
                        //process.exit(0)
                    })
            })
    }

    function shutdown() {
        // `sync` can be undefined if you start the process while offline and
        // then immediately Ctrl-C it before you go online
        if (sync) {
            // close gracefully
            sync.cancel();
        }

        Promise.all([
            db.close(),
            skimLocal.close()
        ]).catch(null).then(() => {
            process.exit();
        });
    }

    function getTarLocation(dist) {
        console.log("get tar ???")
        return new Promise((resolve, reject) => {
            //  if (dist.info) {
            //    console.log("dist.info",dist.info)
            //  request(dist.info, (error, response, body) => {
            //    if (error) return reject(error);
            //  resolve(body.dist.tarball)
            //});
            //} else {
            resolve(dist.tarball);
            //}
        });
    }

    function downloadTar(id) {
        return new Promise((resolve, reject) => {
            var match = (/-\d+\.\d+\.\d+/.exec(id))
            var name = id.slice(0,match.index);
            var version = id.slice(match.index+1);
            var intervalDownloadUrl = FAT_REMOTE + "/tarballs/"+ name + "/" + version + ".tgz"
            console.log(intervalDownloadUrl)
            const options = {
                url: intervalDownloadUrl,
                encoding: null,
                timeout:200,
            };
            request(options, (error, response, body) => {
                if(!error && response.statusCode === 200){
                    resolve(body)
                }else{
                    console.log("???????????????tgz??????")
                    reject()
                }
            });
        });
    }

    //
    // actual server logic
    //
    app.get('/:name/:version', (req, res) => {
        console.log("????????????",req.params.name,req.params.version)
        const name = req.params.name;
        const version = req.params.version;

        getDocument(name)
            .then((doc) => {
                var packageMetadata = massageMetadata(localBase, doc);
                var versionMetadata = findVersion.findVersion(packageMetadata, version);
                if (versionMetadata) {
                    cacheResponse(res, doc._rev);
                    res.json(versionMetadata);
                } else {
                    res.status(404).json({
                        error: 'version not found: ' + version
                    });
                }
            })
            .catch((error) => {
                res.status(500).json({
                    error
                });
            });
    });

    app.get('/:name', (req, res) => {
        console.log("??????name??????",req.params.name)
        const name = req.params.name;
        getDocument(name)
            .then((doc) => {
                console.log(doc.time.modified,"???????????? getDocument")
                res.json(massageMetadata(localBase, doc));
            })
            .catch((error) => {
                res.status(500).json({
                    error
                });
            });
    });

    app.get('/tarballs/:name/:version.tgz', (req, res) => {
        console.log("????????????  /tarballs/:name/:version.tgz")
        var hash = crypto.createHash('sha1');
        var pkgName = req.params.name;
        var pkgVersion = req.params.version;
        var id = `${pkgName}-${pkgVersion}`;
        console.log("???????????????id",id)
        getDocument(pkgName)
            .then((doc) => {
                /**
                 *  ?????????????????????????????????????????????????????????????????????????????????????????????
                 *  ?????????????????????????????????????????????????????????????????????????????????????????????????????????
                 */
                return downloadTar(id)
                    .then((tar) => {
                        console.log(id,"downloadTar:?????????????????????????????????")
                        console.log(tar)
                        db.put(id, tar)
                            .then(() => {
                                console.log(id,"downloadTar:tgz???????????????")
                                return new Promise(function(resolve, reject){
                                    resolve()
                                })
                            })
                            .then(() => {
                                console.log(id,"downloadTar:???doc???????????????")
                                skimLocal.get(pkgName)
                                    .then((_doc) => {
                                        return skimLocal.remove(_doc)
                                            .then(() => {
                                                console.log(id,"???doc????????????")
                                                // ?????????????????????????????????????????????????????????latest????????????
                                                if(_doc.firstimport)
                                                    _doc.versions = {}
                                                delete _doc._rev;
                                                delete _doc.firstimport;
                                                delete _doc.versions[pkgVersion];
                                                _doc.versions[pkgVersion] = doc.versions[pkgVersion];
                                                _doc['dist-tags']['latest'] = findVersion.findLatest(_doc.versions);
                                                console.log(id,"downloadTar:???doc?????????????????????")
                                                return skimLocal.put(_doc);
                                            })
                                    })
                                    .catch((error) => {
                                        console.log(id,"???????????????????????????",error)
                                    })
                            })
                            .then(() => {
                                sendBinary(res,tar)
                            })
                            .catch((error) => {
                                console.log(id,"db.put then ??????????????????",error)
                                res.status(500).send(error);
                            });
                    })
                    .catch((error) => {
                        console.log(id,"downloadTar:??????????????????????????????????????????tgz???",error)
                        var dist = doc.versions[pkgVersion].dist;
                        return db.get(id, {
                            asBuffer: true,
                            valueEncoding: 'binary'
                        })
                            .then((buffer) => {
                                console.log("?????????????????????",buffer.length)
                                if(buffer.length < 100){
                                    console.log("??????????????????,????????????")
                                    res.status(500).send({
                                        error: 'hashes don\'t match, not returning'
                                    })
                                }
                                hash.update(buffer);
                                if (dist.shasum !== hash.digest('hex')) {
                                    // happens when we write garbage to disk somehow
                                    res.status(500).send({
                                        error: 'hashes don\'t match, not returning'
                                    })
                                } else {
                                    logger.hit(pkgName, pkgVersion);
                                    return sendBinary(res, buffer);
                                }
                            })
                            .catch((error) => {
                                console.log(id,"??????????????????????????????tgz?????????????????????",error)
                                //process.exit(0)
                            })
                    })
            })
            .catch((error) => {
                console.log("??????tar?????? getDocument ????????????")
                console.log(error)
                res.status(500).send(error);
            });
        // getDocument(pkgName)
        //     .then((doc) => {
        //         var dist = doc.versions[pkgVersion].dist;
        //         return db.get(id, {
        //             asBuffer: true,
        //             valueEncoding: 'binary'
        //         })
        //         .then((buffer) => {
        //             console.log("?????????????????????",buffer.length)
        //             if(buffer.length < 10){
        //                 console.log("??????????????????,????????????")
        //                 res.status(500).send({
        //                     error: 'hashes don\'t match, not returning'
        //                 })
        //             }
        //             hash.update(buffer);
        //             if (dist.shasum !== hash.digest('hex')) {
        //                 // happens when we write garbage to disk somehow
        //                 res.status(500).send({
        //                     error: 'hashes don\'t match, not returning'
        //                 })
        //             } else {
        //                 logger.hit(pkgName, pkgVersion);
        //                 return sendBinary(res, buffer);
        //             }
        //         })
        //         .catch(() => {
        //             console.log("???tgz???????????????")
        //             logger.miss(pkgName, pkgVersion);
        //             return getTarLocation(dist)
        //                 .then((location) => {
        //                     console.log("??????????????????")
        //                     return downloadTar(id, location)
        //                 })
        //                 .then((tar) => {
        //                     sendBinary(res, tar);
        //                 })
        //                 .catch((error) => {
        //                     res.status(500).send(error);
        //                 });
        //         })
        //     })
        //     .then(() => {
        //         return skimLocal.get(pkgName);
        //     })
        //     .then((doc) => {
        //         doc.versions[pkgVersion].downloads ? doc.versions[pkgVersion].downloads += 1 : doc.versions[pkgVersion].downloads = 1;
        //         return skimLocal.put(doc);
        //     })
        //     .catch((error) => {
        //         res.status(500).send({
        //             error
        //         });
        //     });
    });

    // allow support for scoped packages
    app.get('/tarballs/:user/:package/:version.tgz', (req, res) => {
        console.log("????????????  /tarballs/:user/:package/:version.tgz")
        var hash = crypto.createHash('sha1');
        var userName = req.params.user;
        var pkgName = req.params.package;
        var pkgVersion = req.params.version;
        var fullName = `${userName}/${pkgName}`;
        var id = `${userName}/${pkgName}-${pkgVersion}`;

        getDocument(fullName)
            .then((doc) => {
                /**
                 *  ?????????????????????????????????????????????????????????????????????????????????????????????
                 *  ?????????????????????????????????????????????????????????????????????????????????????????????????????????
                 */
                return downloadTar(id)
                    .then((tar) => {
                        console.log(id,"downloadTar:?????????????????????????????????")
                        console.log(tar)
                        db.put(id, tar)
                            .then(() => {
                                console.log(id,"downloadTar:tgz???????????????")
                                return new Promise(function(resolve, reject){
                                    resolve()
                                })
                            })
                            .then(() => {
                                console.log(id,"downloadTar:???doc???????????????")
                                skimLocal.get(fullName)
                                    .then((_doc) => {
                                        return skimLocal.remove(_doc)
                                            .then(() => {
                                                console.log(id,"???doc????????????")
                                                // ?????????????????????????????????????????????????????????latest????????????
                                                if(_doc.firstimport)
                                                    _doc.versions = {}
                                                delete _doc._rev;
                                                delete _doc.firstimport;
                                                delete _doc.versions[pkgVersion];

                                                _doc.versions[pkgVersion] = doc.versions[pkgVersion];
                                                _doc['dist-tags']['latest'] = findVersion.findLatest(_doc.versions);
                                                console.log(id,"downloadTar:???doc????????????????????????")
                                                return skimLocal.put(_doc);
                                            })
                                    })
                                    .catch((error) => {
                                        console.log(id,"???????????????????????????",error)
                                    })
                            })
                            .then(() => {
                                sendBinary(res,tar)
                            })
                            .catch((error) => {
                                console.log(id,"db.put then ??????????????????",error)
                                res.status(500).send(error);
                            });
                    })
                    .catch((error) => {
                        console.log(id,"downloadTar:??????????????????????????????????????????tgz???")
                        var dist = doc.versions[pkgVersion].dist;
                        return db.get(id, {
                            asBuffer: true,
                            valueEncoding: 'binary'
                        })
                            .then((buffer) => {
                                console.log("?????????????????????",buffer.length)
                                if(buffer.length < 100){
                                    console.log("??????????????????,????????????")
                                    res.status(500).send({
                                        error: 'hashes don\'t match, not returning'
                                    })
                                }
                                hash.update(buffer);
                                if (dist.shasum !== hash.digest('hex')) {
                                    // happens when we write garbage to disk somehow
                                    res.status(500).send({
                                        error: 'hashes don\'t match, not returning'
                                    })
                                } else {
                                    logger.hit(fullName, pkgVersion);
                                    return sendBinary(res, buffer);
                                }
                            })
                            .catch((error) => {
                                console.log(id,"??????????????????????????????tgz?????????????????????")
                                process.exit(0)
                            })
                    })
            })
            .catch((error) => {
                console.log("??????tar?????? getDocument ????????????")
                console.log(error)
                res.status(500).send(error);
            });

        // getDocument(fullName)
        //     .then((doc) => {
        //         var dist = doc.versions[pkgVersion].dist;
        //
        //         return db.get(id, {
        //             asBuffer: true,
        //             valueEncoding: 'binary'
        //         }).then((buffer) => {
        //             if(buffer.length < 10){
        //                 console.log("??????????????????,????????????")
        //                 res.status(500).send({
        //                     error: 'hashes don\'t match, not returning'
        //                 })
        //             }
        //             hash.update(buffer);
        //             if (dist.shasum !== hash.digest('hex')) {
        //                 // happens when we write garbage to disk somehow
        //                 res.status(500).send({
        //                     error: 'hashes don\'t match, not returning'
        //                 })
        //             } else {
        //                 logger.hit(pkgName, pkgVersion);
        //                 return sendBinary(res, buffer);
        //             }
        //         })
        //             .catch(() => {
        //                 logger.miss(pkgName, pkgVersion);
        //
        //                 return getTarLocation(dist)
        //                     .then((location) => {
        //                         return downloadTar(id, location)
        //                     })
        //                     .then((tar) => {
        //                         sendBinary(res, tar);
        //                     })
        //                     .catch((error) => {
        //                         res.status(500).send(error);
        //                     });
        //             })
        //     })
        //     .then(() => {
        //         return skimLocal.get(pkgName);
        //     })
        //     .then((doc) => {
        //         doc.versions[pkgVersion].downloads ? doc.versions[pkgVersion].downloads += 1 : doc.versions[pkgVersion].downloads = 1;
        //         return skimLocal.put(doc);
        //     })
        //     .catch((error) => {
        //         res.status(500).send({
        //             error
        //         });
        //     });
    });

    app.put('/*', proxy(FAT_REMOTE, {
        limit: Infinity
    }));

    var sync;

    function replicateSkim() {
        skimRemote.info()
            .then((info) => {
                sync = skimLocal.replicate.from(skimRemote, {
                    live: true,
                    batch_size: 200,
                    retry: true
                }).on('change', (change) => {
                    startingTimeout = 1000;
                    var percent = Math.min(100,
                        (Math.floor(change.last_seq / info.update_seq * 10000) / 100).toFixed(2));
                    logger.sync(change.last_seq, percent);
                }).on('error', (err) => {
                    // shouldn't happen
                    logger.warn(err);
                    logger.warn('Error during replication with ' + SKIM_REMOTE);
                });
            }).catch((err) => {
            logger.warn(err);
            logger.warn('Error fetching info() from ' + SKIM_REMOTE +
                ', retrying after ' + Math.round(startingTimeout) + ' ms...');
            //restartReplication();
        });
    }

    function restartReplication() {
        // just keep going
        startingTimeout *= backoff;
        setTimeout(replicateSkim, Math.round(startingTimeout));
    }

    function getCount() {
        return new Promise((fulfill, reject) => {
            var i = 0;
            db.createKeyStream()
                .on('data', () => {
                    i++;
                }).on('end', () => {
                fulfill(i);
            }).on('error', reject);
        });
    }
    //replicateSkim();

    process.on('SIGINT', () => {
        shutdown();
    });

    return {
        server: app.listen(port, callback),
        shutdown
    }
};