drone-build-log:
	printenv
	echo "Branch: ${DRONE_COMMIT_REF}\nCommit: ${DRONE_COMMIT_SHA}\nBuild Number:${DRONE_BUILD_NUMBER} \nTime: "`TZ=Asia/Shanghai date "+%Y-%m-%d %H:%M:%S"` > ./ci.txt
	cat ./ci.txt
