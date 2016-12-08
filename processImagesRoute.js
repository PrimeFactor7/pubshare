"use strict";

// Sample code file from a complete project - a feed processer batch process
// Route for image processing.  Converts images directed by to predefined template definitions
// Uses async library, and custom batch processing

var express = require('express');
var router = express.Router();
var FeedEntity = require('../models/feed.js');
const Feed = new FeedEntity();
var PostEntity = require('../models/post.js');
const Post = new PostEntity();
var BatchEntity = require('../models/batch');
var Batch = new BatchEntity();
var BatchItemEntity = require('../models/batchItem');
var BatchItem = new BatchItemEntity();
var ImageEntity = require('../models/image.js');
var Image = new ImageEntity();
var ImageGenerationEntity = require('../models/imageGeneration.js');
var ImageGeneration = new ImageGenerationEntity();
var async = require('async');
var cnUtil = require('../modules/cn-util');
var cnUtilStorage = require('../modules/cn-util-storage');
var logicalCallStack = require('../modules/logicalCallStack');
var log = require('../modules/logger');
var sharp = require('sharp');
var StatClass = require('../modules/stat.js');

function resizeImage(feed, post, image, targetW, targetH, minOrMax, crop, quality, ext, index, callbackFn) {
    var format = null;
    if (ext == 'jpg')
        format = 'jpeg';
    if (minOrMax == 'min') {
        return sharp(image)
            .resize(targetW, targetH).min().quality(quality)
            .crop(sharp.gravity.center)
            .toFormat(format)
            .toBuffer(function (err, resizedImage) {
                if (err || !resizedImage || resizedImage.length == 0) {
                    var errImageResize = {
                        message: 'Image Resize Failed : ' + (err ? err.message : ''),
                        stack: (err ? err.stack : null)
                    };
                    err = err || errImageResize;
                    log.error('Error', err);
                    callbackFn(err);
                }
                else {
                    //log.debug('Image Resize Succeeded');
                    try {
                        return sharp(resizedImage)
                            .metadata()
                            .then(function (metadata) {
                                    cnUtilStorage.saveImageToFile(feed, post, index, targetW, targetH, ext, resizedImage, function (errSaveImage) {
                                        metadata.size = resizedImage.length;
                                        callbackFn(errSaveImage, metadata);
                                    });
                                },
                                function (errSharp) {
                                    // TODO : handle other image types
                                    var err = {message: 'Error: Sharp metadata failure on converted image : ' + errSharp};
                                    log.error('Error', err);
                                    callbackFn(err);
                                })
                            .catch(function (ex) {
                                // TODO : handle other image types
                                var err = {message: 'Error: Sharp metadata failure on converted image : ' + ex.message};
                                log.error('Error', err);
                                callbackFn(err);
                            });
                    }
                    catch (ex) {
                        // TODO : handle other image types
                        var err = {message: 'Error: Sharp metadata failure on converted image : ' + ex.message};
                        log.error('Error', err);
                        callbackFn(err);
                    }
                }
            });
    }
    // 'max', if needed
}

function convertImage(feed, post, cnImage, igen, index, statImageConversions, callbackFn) {
    cnUtilStorage.readImageFromFile(feed, post, cnImage.index, cnImage.width, cnImage.height, cnImage.extension, function (errReadImage, image) {
        if (errReadImage) {
            log.error('Error reading image file for conversion', errReadImage, post.id, post.title, feed.id, feed.provider, cnImage);
            callbackFn(errImageResize);
        }
        else {
            resizeImage(feed, post, image, igen.width, igen.height, igen.minOrMax, igen.crop, igen.quality, igen.extDest, index, function (errImageResize, imageMetadata) {
                if (!errImageResize) {
                    statImageConversions.inc(imageMetadata.size);
                    cnUtilStorage.addImage(feed, post, 99, igen.width, igen.height,
                        igen.extDest, cnImage.extension, cnUtil.cnImageType(cnImage.cnImageType.charAt(0), cnImage.cnImageType.charAt(1), 'r'),
                        igen.useForMainImage, igen.minOrMax, igen.cropped, igen.quality, imageMetadata.fileSize, function (errAddImage) {
                            callbackFn(errAddImage);
                        });
                }
                else {
                    callbackFn(errImageResize);
                }
            });
        }
    });
}


function updatePost(post, cb, lastErr) {
    Post.updateOne(post.id, post, (err) => {
        if (err) {
            log.error('Error on Post Save : ' + JSON.stringify(err));
            cb(err);
        }
        else {
            async.nextTick(function () {
                cb(lastErr);
            });
        }
    });
}

function resizeImages(feedValue, batch, lcsBatch, statImageConversions, cbResizeImages) {
    var feed = feedValue;
    BatchItem.addTask(feed, batch, lcsBatch, (err, batchItem, lcsBatchItem) => {
        log.trace('Process Images : ' + feed.provider + ' : ' + feed.sourceTag);
        //TODO: add BackPropagate and Purge image generation
        Post.findByStatus(feed.id, batch.type, (err, posts) => {
                if (err) {
                    log.error('Error on Posts Find : ' + JSON.stringify(err));
                    cbResizeImages(err);
                }
                else {
                    if (posts) {
                        log.info('Process Images : Feed : ' + feed.provider + (feed.sourceTag ? ' : ' + feed.sourceTag : '') + ' : ' + 'Post Count : '
                            + (posts ? posts.length : 0));

                        async.eachLimit(posts, 20,
                            function (postEach, cbEachSeries) {
                                // Set the imagesProcessed, as in post was processed for image conversion.
                                // Individual image conversions or all may still have failed per hasResizedImages
                                postEach.imagesProcessed = true;
                                postEach.hasResizedImages = false;

                                async.waterfall([
                                        async.constant(postEach),
                                        function (post, cbWaterfallExtract) {
                                            var lcsResizeImage = lcsBatchItem.add(
                                                {name: 'Resize Image', idExternal: post.id, log: log});

                                            // Prefer the extracted main image if it exists
                                            var index = (post.hasExtractedMainImage ? 1 : 0);
                                            if (post.hasPostMainImage || post.hasExtractedMainImage) {
                                                Image.findOne({
                                                    postId: post.id,
                                                    index: index
                                                }, (errImageFind, cnImage) => {
                                                    if (errImageFind || !cnImage) {
                                                        var err = new Error('Process Images - No Image record returned : ' + feed.provider + (errImageFind ? errImageFind.status : ''));
                                                        err.status = "Process Images Error";
                                                        lcsResizeImage.end(err);
                                                        cbWaterfallExtract(err, post);
                                                    }
                                                    else {
                                                        ImageGeneration.find({active: true}, {}, true, (errIGens, igens) => {
                                                            if (errIGens) {
                                                                lcsResizeImage.end(errIGens);
                                                                cbWaterfallExtract(errIGens, post);
                                                            }
                                                            else {
                                                                async.eachSeries(
                                                                    igens,
                                                                    function (igen, cbIGenSeries) {
                                                                        convertImage(feed, post, cnImage, igen, 99, statImageConversions, function (errConvert) {
                                                                            if (errConvert) {
                                                                                if (post.hasResizedImages == null) {
                                                                                    post.hasResizedImages = false;
                                                                                }
                                                                                statImageConversions.incErr();
                                                                                cbIGenSeries(errConvert);
                                                                            }
                                                                            else {
                                                                                post.hasResizedImages = true;
                                                                                cbIGenSeries();
                                                                            }
                                                                        });
                                                                    },
                                                                    function (err) {
                                                                        log.debug('IGen Series complete');
                                                                        lcsResizeImage.end(err);
                                                                        cbWaterfallExtract(null, post);
                                                                    }
                                                                );
                                                            }
                                                        });
                                                    }
                                                });
                                            }
                                            else {
                                                lcsResizeImage.end();
                                                cbWaterfallExtract(null, post);
                                            }
                                        }
                                    ],
                                    // cbWaterfallExtract
                                    function (err, post) {
                                        if (err) {
                                            batchItem.status = 'Error';
                                            batchItem.error = err.message;
                                            batchItem.stack = JSON.stringify(err.stack);
                                        }
                                        // update post when all images are converted for the post
                                        updatePost(post, cbEachSeries, null);
                                    }
                                );
                            },
                            function (err) {
                                log.info('Process Images : Feed : ' + feed.provider + ' : ' + feed.sourceTag + ' : Complete');
                                Batch.completeBatchItem(batch, batchItem, lcsBatchItem, err);
                                cbResizeImages();
                            }
                        );
                    }
                    else {
                        log.debug('Feed complete : no posts to process');
                        Batch.completeBatchItem(batch, batchItem, lcsBatchItem, err);
                        cbResizeImages();
                    }
                }
            }
        );
    });
}

router.get('/processimages/:schedule_id/:start_row/:end_row', function (req, res, next) {
    var scheduleId = cnUtil.coerceNull(req.params.schedule_id);
    var startRow = parseInt(req.params.start_row);
    var endRow = parseInt(req.params.end_row);
    var statProcessImages = new StatClass('p', 'processimages');
    var statImageConversions = new StatClass('p', 'Image Conversions');

    function endStats() {
        statProcessImages.saveProcess(err => {
            statImageConversions.saveProcess(err => {
            });
        });
    }

    Batch.addTask('Process Images', scheduleId, 'Pending', (err, batch, lcsBatch) => {
        res.send({success: true, message: 'Initiated', batch: batch});

        Feed.getAllActivePartition(startRow, endRow, (err, feeds) => {
            if (err) {
                log.error('Error on Feed Find : ' + JSON.stringify(err));
                Batch.completeTask(batch, lcsBatch, err, (errTask)=> endStats());
            }
            else {
                if (!feeds || feeds.length == 0) {
                    Batch.completeTask(batch, lcsBatch, new Error('No Feeds'), (errTask)=> endStats());
                }
                else {
                    batch.pendingItems = feeds.length;
                    Batch.updateStatus(batch, 'Processing');
                    async.eachLimit(
                        feeds,
                        15,
                        function (feed, cbEachSeries) {
                            resizeImages(feed, batch, lcsBatch, statImageConversions,
                                function (err) {
                                    if (err) {
                                        log.error('Error processing posts array : ' + err.toString());
                                    }
                                    else {
                                        log.debug('Feed complete : ' + feed.provider + ' : ' + feed.sourceTag);
                                    }
                                    async.nextTick(function () {
                                        cbEachSeries(err);
                                    });
                                });
                        },
                        function (err) {
                            if (err) {
                                log.error('Error processing posts array : ' + err.toString());
                            }
                            Batch.completeTask(batch, lcsBatch, err, (errTask)=> endStats());
                        });
                }
            }
        });
    });
});

module.exports = router;
