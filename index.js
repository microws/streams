"use strict";
const stream = require("stream");
const through2 = require("through2");
const pump = require("pump");
const pumpify = require("pumpify");
const util = require("util");
const write = require("./lib/flushwrite.js");
const zlib = require("zlib");
const split = require("split2");

let ls = module.exports = {
    passthrough: (opts) => {
        return stream.PassThrough({
            highWaterMark: 16,
            objectMode: true,
            ...opts
        });
    },
    through: (func, params = {}) => {
        const { opts, flush, forcePromise, commands, cmdWrap } = Object.assign({ commands: {}, cmdWrap: true, forcePromise: false }, params);
        let t = through2.obj(opts, wrapThroughTransform(func, forcePromise, cmdWrap, commands), wrapThroughFlush(flush, forcePromise));
        t._stop = t._transform.stop;
        t.onStop = t._transform.onStop;
        t._transform.bind(t);
        return t;
    },
    write: (func, params = {}) => {
        const { opts, flush, forcePromise, commands, cmdWrap } = Object.assign({ commands: {}, cmdWrap: true, forcePromise: false }, params);
        let w = write.obj(opts, wrapWriteTransform(func, forcePromise, cmdWrap, commands), wrapWriteFlush(flush, forcePromise));
        w._stop = w._worker.stop;
        w.onStop = w._worker.onStop;
        w._worker.bind(w);
        return w;
    },
    pipe: function () {
        let streams = Array.prototype.slice.call(arguments);
        streams.forEach((stream, index) => {
            stream.stop = function () {
                for (let i = index; i >= 0; i--) {
                    if (streams[i]._stop) {
                        streams[i]._stop();
                    } else if (typeof streams[i].destroy === "function") {
                        streams[i].destroy();
                    } else if (typeof streams[i].close === "function") {
                        streams[i].close();
                    }
                }
            }
        });
        return util.promisify(pump).apply(pump, streams);
    },
    pipeline: function () {
        let streams = Array.prototype.slice.call(arguments);

        let p = pumpify.obj.apply(pumpify.obj, streams);
        p._stop = function () {
            for (let i = streams.length - 1; i >= 0; i--) {
                if (streams[i]._stop) {
                    streams[i]._stop();
                } else if (typeof streams[i].destroy === "function") {
                    streams[i].destroy();
                } else if (typeof streams[i].close === "function") {
                    streams[i].close();
                }
            }
        }
        return p;
    },

    log: function (prefix, stringify = false) {
        let log = console.log;
        if (prefix) {
            log = function () {
                console.log.apply(null, [prefix].concat(Array.prototype.slice.call(arguments)));
            };
        }
        return ls.through((obj, done) => {
            if (!stringify || typeof obj === "string") {
                log(obj);
            } else {
                log(JSON.stringify(obj, null, 2));
            }
            done(null, obj);
        }, {
            cmdWrap: false,
            flush: (done) => {
                console.log(prefix, `flush called`);
                done();
            }
        }
        );
    },
    logField: function (field, prefix = 'field') {
        return ls.through((obj, done) => {
            console.log(prefix, obj[field]);
            done(null, obj);
        })
    },
    devnull: function (shouldLog = false) {
        if (shouldLog) {
            return ls.pipeline(
                ls.log(shouldLog === true ? "devnull" : shouldLog),
                ls.write((obj, done) => done())
            );
        } else {
            return ls.write((obj, done) => done());
        }
    },
    split: split,
    gunzip: function () {
        let gzip = zlib.createGunzip();
        gzip.setEncoding('utf8');
        gzip.on("error", (err) => {
            console.log(err);
        })
        return gzip;
    },
    fromJson: () => ls.through((obj, done) => {
        let o;
        try {
            o = JSON.parse(obj);
        } catch (e) {
            console.log("from json error", e);
            return done(e);
        }
        return done(null, o);
    }),
    stringify: () => ls.through((obj, done) => {
        done(null, JSON.stringify(obj) + "\n");
    }, (done) => {
        console.log("FLUSH stringify");
        done();
    }),
    limit: ({ size }) => {
        let s;
        if (size) {
            let currentSize = 0;
            let byteSizeLimit = 0;
            if (typeof size == "number") {
                byteSizeLimit = size;
            } else {
                let multiplication = {
                    bytes: 1,
                    kb: 1024,
                    mb: 1024 * 1024,
                    gb: 1024 * 1024 * 1024,
                    tb: 1024 * 1024 * 1024 * 1024
                };
                Object.keys(size).forEach(n => {
                    byteSizeLimit += Math.floor(multiplication[n.toLowerCase()] * size[n]);
                })
            }
            s = ls.through((obj, done) => {
                currentSize += obj.length;
                if (currentSize > byteSizeLimit) {
                    s.stop();
                    done(null);
                } else {
                    done(null, obj);
                }
            });
        }
        return s;
    },
    count: (label = "count") => {
        let count = 0;
        return ls.through((obj, done) => {
            count++;
            done(null, obj);
        }, {
            flush: done => {
                console.log(label, count);
                done();
            }
        }
        );
    },
    parse: () => {
        return ls.pipeline(ls.split(), ls.fromJson());
    },
    gunzipParse: () => {
        return ls.pipeline(ls.gunzip(), ls.split(), ls.fromJson());
    },
    fromString: (string) => {
        let pass = stream.PassThrough();
        pass.end(string);
        return pass;
    },
    batch: ({ }) => {

    }
};


function wrapThroughTransform(func, forcePromise, cmdWrap, commands) {
    let usesPromises = forcePromise || util.types.isAsyncFunction(func);
    let wrappedFunc;
    let stopped = false;
    let stopFunc = null;

    let onStop = (f) => {
        stopFunc = f;
    };
    if (cmdWrap && usesPromises) {
        wrappedFunc = function (obj, enc, done) {
            if (stopped) {
                done();
            } else if (obj._cmd) {
                if (obj._cmd in commands) {
                    commands[obj._cmd](obj, {
                        push: this.push.bind(this),
                    }).then(result => {
                        done(null, result)
                    });
                } else {
                    done(null, obj);
                }
            } else {
                func(obj, {
                    push: this.push.bind(this),
                }).then(result => {
                    done(null, result)
                });
            }
        };
    } else if (cmdWrap) {
        wrappedFunc = function (obj, enc, done) {
            if (stopped) {
                done();
            } else if (obj._cmd) {
                if (obj._cmd in commands) {
                    commands[obj._cmd](obj, done, {
                        push: this.push.bind(this),
                    });
                } else {
                    done(null, obj);
                }
            } else {
                func(obj, done, {
                    push: this.push.bind(this),
                    stream: this,
                });
            }
        }
    } else if (usesPromises) {
        wrappedFunc = function (obj, enc, done) {
            if (stopped) {
                done();
            } else {
                func(obj, done, {
                    push: this.push.bind(this),
                }).then(result => done(null, result));
            }
        }
    } else {
        wrappedFunc = function (obj, enc, done) {
            if (stopped) {
                done();
            } else {
                func(obj, done, {
                    push: this.push.bind(this),
                });
            }
        }
    }
    wrappedFunc.stop = function () {
        stopped = true;
        if (stopFunc) {
            stopFunc();
        }
    }
    wrappedFunc.onStop = function (func) {
        stopFunc = func;
    }
    return wrappedFunc;
}
function wrapWriteTransform(func, forcePromise, cmdWrap, commands) {
    let usesPromises = forcePromise || util.types.isAsyncFunction(func);
    let wrappedFunc;
    let stopped = false;
    let stopFunc = null;

    let onStop = (f) => {
        stopFunc = f;
    };
    if (cmdWrap && usesPromises) {
        wrappedFunc = function (obj, enc, done) {
            if (stopped) {
                done();
            } else if (obj._cmd) {
                if (obj._cmd in commands) {
                    commands[obj._cmd](obj, {
                    }).then(result => done());
                } else {
                    done();
                }
            } else {
                func(obj, {
                }).then(result => {
                    done()
                });
            }
        };
    } else if (cmdWrap) {
        wrappedFunc = function (obj, enc, done) {
            if (stopped) {
                done();
            } else if (obj._cmd) {
                if (obj._cmd in commands) {
                    commands[obj._cmd](obj, done, {
                    });
                } else {
                    done();
                }
            } else {
                func(obj, done, {
                });
            }
        }
    } else if (usesPromises) {
        wrappedFunc = function (obj, enc, done) {
            if (stopped) {
                done();
            } else {
                func(obj, done, {
                }).then(result => done());
            }
        }
    } else {
        wrappedFunc = function (obj, enc, done) {
            if (stopped) {
                done();
            } else {
                func(obj, done, {
                });
            }
        }
    }
    wrappedFunc.stop = function () {
        stopped = true;
        if (stopFunc) {
            stopFunc();
        }
    }
    wrappedFunc.onStop = function (func) {
        stopFunc = func;
    }
    return wrappedFunc;
}
function wrapThroughFlush(flush, forcePromise, isWrite = false) {
    let usesPromises = forcePromise || util.types.isAsyncFunction(flush);
    if (flush) {
        if (usesPromises) {
            return function (done) {
                return flush(this.push && this.push.bind(this)).then(result => {
                    done(null, result);
                });
            }
        } else {
            return function (done) {
                return flush(done, this.push && this.push.bind(this));
            };
        }
    } else {
        return null
    }
}
function wrapWriteFlush(flush, forcePromise, isWrite = false) {
    let usesPromises = forcePromise || util.types.isAsyncFunction(flush);
    if (flush) {
        if (usesPromises) {
            return function (done) {
                return flush().then(result => done(null));
            }
        } else {
            return function (done) {
                return flush(done);
            };
        }
    } else {
        return null
    }
}
