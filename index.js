var follow = require('cloudant-follow');
const normalize = require('normalize-registry-metadata');
const fsExtra = require('fs-extra');
const got = require('got');
const pAll = require('p-all');
const resolve = require('path').resolve;

if (typeof process.argv[2] !== 'string') {
  console.error(`Please provide a 'since' value as an argument. To start from the beginning, use '0'.
Example:
node index.js 0`);
  process.exit(1);
}

const db = 'http://127.0.0.1:5984/registry';
const bufferSize = 100;
const downloadConcurrency = 20;
const dataDir = process.env.NPM_ARCHIVER_DATA
  ? resolve(process.env.NPM_ARCHIVER_DATA)
  : resolve(process.cwd(), 'tarballs');
console.log('Data directory set to: ', dataDir);

fsExtra.ensureDirSync(dataDir);

let since = process.argv[2];
let nextSince = '';
let buffer = [];

function die(err) {
  console.error(err);
  console.log(`Last known change sequence number: ${since}`);
  process.exit(1);
}

function processBuffer(processed) {
  const prep = buffer.map(async change => {
    const normalized = normalize(change.doc);
    if (normalized === undefined) {
      return undefined;
    }
    const name = normalized.name;
    const versions = Object.values(normalized.versions);
    const tarballs = versions.map(version => version.dist.tarball);
    const directory = resolve(dataDir, name);
    await fsExtra.ensureDir(directory);
    return tarballs.map(url => ({ directory, url }));
  });

  Promise.all(prep)
    .then(result => {
      const tasks = [].concat(...result).filter(value => value !== undefined);
      const downloads = tasks.map(task => async () => {
        const tarballName = task.url.split('/').pop();
        const path = resolve(task.directory, tarballName);
        const previouslyDownloaded = await fsExtra.pathExists(path);
        if (!previouslyDownloaded) {
          new Promise(resolve => {
            got
              .stream(task.url)
              .on('error', err => {
                console.error(
                  `Error downloading ${tarballName} from ${task.url}: ${err}`
                );
                fsExtra.remove(path);
              })
              .on('close', resolve)
              .pipe(fsExtra.createWriteStream(path));
          });
        }
      });
      pAll(downloads, { concurrency: downloadConcurrency }).then(() => {
        since = nextSince;
        console.log(
          `Completed processing for change sequence number: ${since}`
        );
        buffer = [];
        processed();
      });
    })
    .catch(err => die(err));
}

const feed = new follow.Feed({ db, since, include_docs: true });

feed.on('change', function(change) {
  buffer.push(change);
  if (buffer.length >= bufferSize) {
    feed.pause();
    nextSince = change.seq;
    processBuffer(() => feed.resume());
  }
});

// Follow always retries on errors, so this is an unrecoverable error
feed.on('error', err => die(err));

feed.follow();
