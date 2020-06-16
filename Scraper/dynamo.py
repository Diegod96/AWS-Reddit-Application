import boto3



def articles_table():

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("articles")
    return table