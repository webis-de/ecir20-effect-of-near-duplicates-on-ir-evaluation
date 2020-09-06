#!/bin/bash -e

TO_LABEL=$(find . -type f -printf '%h\n'|grep 'cc-news-2020-01-02-03-04' | sort | uniq -d)
function process_dir()
{
	echo "Process ${DIR}"
	FIRST_FILE=$(echo "full-text-$(echo ${DIR:27}|awk -F '_vs_' '{print $1}')")
	SECOND_FILE=$(echo "full-text-$(echo ${DIR:27}|awk -F '_vs_' '{print $2}')")

	echo "meld ${DIR}/${FIRST_FILE} ${DIR}/${SECOND_FILE} -o ${DIR}/label"
	meld ${DIR}/${FIRST_FILE} ${DIR}/${SECOND_FILE} -o ${DIR}/label
}


for DIR in ${TO_LABEL[@]}
do
	if [ "6" = "$(find ${DIR}|wc -l)" ]
	then
		process_dir ${DIR}
	else
		echo "Done: ${DIR}"
	fi
done
