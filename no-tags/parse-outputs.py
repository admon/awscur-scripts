import os, errno
import json
import requests

workdir="/home/cloud/map-migrated/outputs"
if not os.path.exists(workdir): raise SystemExit("Directory outputs not exists!")
os.chdir(workdir) # Fixed working dir

excluded_pattern=["aws-tagging-automation", "cf-templates"]
rgname={ "ap-southeast-1": "SG", "ap-northeast-1":"JP", "ap-east-1":"HK" }
aacct={
        "017820703885": "muda-stg01",
        "017820653885": "muda-sim",
        "018725633885": "cloud-poc"
}

# type of resources to monitor
charged_type=["backup:backupplan", "cloudfront:distribution", "dms:replicationinstance", "dms:replicationtask", "dynamodb:table", "ec2:volume", "ec2:instance", "ec2:natgateway", "ec2:image", "ec2:eip", "ec2:transitgateway", "ec2:vpcendpoint", "ecs:cluster", "eks:cluster", "elasticache:cachecluster", "elasticache:snapshot", "efs:filesystem", "elasticloadbalancingv2:loadbalancer", "elasticloadbalancingv2:loadbalancer", "elasticloadbalancingv2:loadbalancer", "elasticsearch:domain", "opensearchservice:domain", "kinesisfirehose:deliverystream", "glue:crawler", "glue:database", "glue:job", "kafka:cluster", "kinesis:stream", "lambda:function", "logs:loggroup", "logs:loggroup", "cloudwatch:dashboard", "cloudwatch:alarm", "rds:dbcluster", "rds:dbinstance", "rds:dbsnapshot", "redshift:cluster", "route53:hostedzone", "s3:bucket", "sagemaker:notebookinstance", "secretsmanager:secret", "sns:topic", "sqs:queue"]

def send_lark(msg):
    url = "https://open.larksuite.com/open-apis/bot/v2/hook/7bxxxxxxxx-xxxxxxxx"
    params = {"receive_id_type":"chat_id"}
    msg_content = {
        "text": msg,
    }
    req = {
        "msg_type": "text",
        "content": json.dumps(msg_content)
    }
    payload = json.dumps(req)
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, params=params, headers=headers, data=payload)
    #print(response.headers['X-Tt-Logid']) # for debug or oncall
    print(response.content)

# check new resource that created in the last 3 days
untagged_output = "untagged-resource"
try:
    os.remove(untagged_output)
except OSError as e:
    if e.errno != errno.ENOENT:
        print("Cannot delete status track file %s" % untagged_output)
        send_lark("Error: cannot delete status track untagged_output: {0}".format(e))

alert_log = open(untagged_output, 'w+')
alert_log.write("Region * Account ***** No migration tag resource.\n")

def parse_single(file):
    jsingle = open(file, 'r').read()
    if len(jsingle) < 7:
        return
    tagjson = json.loads(jsingle)

    for res in tagjson["Resources"]:
        dgood=res["Arn"].split(":")

        if res["Region"] not in rgname.keys():
            continue
        if dgood[4] == "851805" or res["OwningAccountId"] == "851805":
            continue
        if res["ResourceType"] not in charged_type:
            continue
        if any(ex in res["Arn"] for ex in excluded_pattern):
            continue

        alert_log.write("%-5s %-18s  %-s\n" % (rgname[res["Region"]], "HK-" + aacct[res["OwningAccountId"]], res["Arn"]))

for jfile in os.listdir():
    if not jfile.endswith('.notag'):
        continue
    parse_single(jfile)

alert_log.close()
with open(untagged_output, 'r') as output:
    untagged = output.read()
    if len(untagged) > 60:
        send_lark(untagged)
