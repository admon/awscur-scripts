#set -e

working_dir="/home/cloud/map-migrated"
puts=${working_dir}/outputs

cd $working_dir || exit

[ -d "$puts" ] && rm -f ${puts}/* || mkdir $puts

# 客户侧 账号 ID 列表
AWSIds=(00064895 00063769 00062925 00062828 00031356 00031812 00061708 00063644 00030998 00031295 00065224 00065522 00065437)


gen_output() {
	[ "$#" -ne 1 ] && {
		echo *********************** ERROR: Only One AccountID Allowed *****************
		exit
	}

	# 获取临时权限
	temp_creds=$(aws sts assume-role \
                  --role-arn "arn:aws:iam::${1}:role/YResourceExplorerQueryRole" \
                  --role-session-name Ycloud-Session \
                  --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
                  --output text)

	export AWS_ACCESS_KEY_ID=$(echo "$temp_creds" | awk '{print $1}')
	export AWS_SECRET_ACCESS_KEY=$(echo "$temp_creds" | awk '{print $2}')
	export AWS_SESSION_TOKEN=$(echo "$temp_creds" | awk '{print $3}')

	# 按 Region 查找样例
	#regns=("ap-southeast-1"  "ap-northeast-1"  "ap-east-1")
	#for rgn in ${regns[@]}
	#do
	#	aws resource-explorer-2 search --query-string="tag:none region:$rgn" >${puts}/${1}-${rgn}.notag-json
	#done
	aws resource-explorer-2 search --query-string="tag:none" >${puts}/${1}.notag-json
	unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
}

for aid in ${AWSIds[@]}
do
	gen_output $aid
	sleep 1
done

# 解析输出，并发送通知信息到 Lark
python3 parse-outputs.py
