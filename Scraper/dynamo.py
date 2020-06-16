import boto3



def articles_table():
    # Connects to DynamoDB and returns the articles table
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("articles")
    return table