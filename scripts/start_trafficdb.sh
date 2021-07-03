# Start mongo daemon for traffic db
# Currently this database is located at the local hard drive.

MONGO_CONFIG=/Users/adelacalle/Documents/mongo_traffic/mongod_traffic.conf
MONGO_PATH=/usr/local/opt/mongodb-community@4.2/bin
echo "Starting mongo daemon for traffic database"
${MONGO_PATH}/mongod --config ${MONGO_CONFIG}
