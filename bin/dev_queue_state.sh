ROOT="$(git rev-parse --show-toplevel)"

export STATEDIR="$ROOT/.dev_queue_state"
export INSTANCE_FILE="$STATEDIR/id"
export CERT_FILE="$STATEDIR/cert.key"
export IP_FILE="$STATEDIR/ip"
export USER_TOKEN_FILE="$STATEDIR/user.token"
export VERSION_FILE="$STATEDIR/version"
