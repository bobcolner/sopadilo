# b2 cli

b2 ls polygon-equities bars/

b2 sync [-h] [--noProgress] [--dryRun] [--allowEmptySource] [--excludeAllSymlinks] [--threads THREADS] \
 		[--compareVersions {none,modTime,size}] [--compareThreshold MILLIS] [--excludeRegex REGEX] \
 		[--includeRegex REGEX] [--excludeDirRegex REGEX] [--excludeIfModifiedAfter TIMESTAMP] \
 		[--destinationServerSideEncryption {SSE-B2,SSE-C}] [--destinationServerSideEncryptionAlgorithm {AES256}] \
 		[--sourceServerSideEncryption {SSE-C}] [--sourceServerSideEncryptionAlgorithm {AES256}] \
 		[--skipNewer | --replaceNewer] [--delete | --keepDays DAYS] source destination

b2 sync --compareVersions=none \
    b2://polygon-equities/bars/renko_v1 \
    /Users/bobcolner/QuantClarity/sopadilo/tmp/local_data/bars/renko_v1

b2 sync \
	b2://polygon-equities/bars/renko_v2 \
	/Users/bobcolner/QuantClarity/sopadilo/tmp/local_data/bars/renko_v2

b2 sync \
    b2://polygon-equities/bars/renko_v3 \
    /Users/bobcolner/QuantClarity/sopadilo/tmp/local_data/bars/renko_v3


