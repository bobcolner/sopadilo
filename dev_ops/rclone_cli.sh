# rclone sync
# Usage:
#   rclone sync source:path dest:path [flags]

rclone sync --progress \
	b2://polygon-equities/bars/renko_v3 \
	/User/bobcolner/QuantClarity/sopadilo/tmp/local_data/bars/renko_v3
