'use strict';

const semver = require('semver');

// find the first version in the package metadata that satisfies
// the query version. e.g. if the latest is 1.2.3, then "1", "1.2", and "1.2.3"
module.exports.findVersion = function (meta, version) {
    if (version === 'latest') {
        const latestVersion = Object.keys(meta.versions)
            .filter((otherVersion) => {
                return semver.valid(otherVersion);
            }).sort((a, b) => {
                return semver.gt(a, b) ? -1 : 1;
            })[0];
        return meta.versions[latestVersion];
    }
    if (meta.versions[version]) {
        return meta.versions[version];
    }
    var versions = Object.keys(meta.versions)
        .filter((otherVersion) => {
            return semver.valid(otherVersion) && semver.satisfies(otherVersion, version);
        }).sort((a, b) => {
            return semver.gt(a, b) ? -1 : 1;
        });
    return meta.versions[versions[0]];
};
module.exports.findLatest = function (versions){
    var versionsArr = Object.keys(versions)
        .filter((otherVersion) => {
            return semver.valid(otherVersion);
        }).sort((a, b) => {
            return semver.gt(a, b) ? 1 : -1;
        })
    let defaultVersion,stableVersion,betaVersion,alphaVersion;
    defaultVersion = versionsArr[versionsArr.length - 1];
    for(let version of versionsArr){
        if(/^\d+\.\d+\.\d+$/.test(version)){
            stableVersion = version;
        }else if(/^\d+\.\d+\.\d+-\w*beta/.test(version)){
            betaVersion = version
        }else if(/^\d+\.\d+\.\d+-\w*alpha/.test(version)){
            alphaVersion = version
        }
    }
    return stableVersion ? stableVersion : betaVersion ? betaVersion : alphaVersion ? alphaVersion : defaultVersion
}
