set -u
step(){ echo; echo "### $*"; }
step "0. 接続先の確認"; aws --version; aws sts get-caller-identity --query Account --output text
step "1. S3: バケット作成→アップロード→読み戻し"
aws s3 mb s3://local-demo2
echo "hello from the emulator" > hello.txt
aws s3 cp hello.txt s3://local-demo2/ --only-show-errors && aws s3 cp s3://local-demo2/hello.txt -
step "2. DynamoDB: 実AWSと同じバリデーションエラー (テーブル名 'ab')"
aws dynamodb create-table --table-name ab --attribute-definitions AttributeName=id,AttributeType=S --key-schema AttributeName=id,KeyType=HASH --billing-mode PAY_PER_REQUEST 2>&1 | tail -1
step "3. DynamoDB: 作成→wait→put→get"
aws dynamodb create-table --table-name demo2 --attribute-definitions AttributeName=id,AttributeType=S --key-schema AttributeName=id,KeyType=HASH --billing-mode PAY_PER_REQUEST --query TableDescription.TableStatus --output text
time aws dynamodb wait table-exists --table-name demo2
aws dynamodb put-item --table-name demo2 --item '{"id":{"S":"item-1"},"msg":{"S":"hi"}}'
aws dynamodb get-item --table-name demo2 --key '{"id":{"S":"item-1"}}' --query Item.msg.S --output text
step "4. SNS→SQS ファンアウト (サービス間連携)"
Q=$(aws sqs create-queue --queue-name demo-q --query QueueUrl --output text); echo "QueueUrl=$Q"
QARN=$(aws sqs get-queue-attributes --queue-url "$Q" --attribute-names QueueArn --query Attributes.QueueArn --output text)
T=$(aws sns create-topic --name demo-t --query TopicArn --output text); echo "TopicArn=$T"
aws sns subscribe --topic-arn "$T" --protocol sqs --notification-endpoint "$QARN" --attributes RawMessageDelivery=true --query SubscriptionArn --output text
aws sns publish --topic-arn "$T" --message "fan-out via SNS" --query MessageId --output text
aws sqs receive-message --queue-url "$Q" --wait-time-seconds 3 --query 'Messages[].Body' --output text
step "5. 未知の操作/存在しないリソースのエラー"
aws s3api head-bucket --bucket does-not-exist 2>&1 | tail -1
