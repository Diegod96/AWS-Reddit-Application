import boto3


def data_chunk(paragraph):
    """
    Break up the text into 5000 byte chunks
    :param paragraph:
    :return: text_list
    """
    text_list = []
    while paragraph:
        text_list.append(str(paragraph[:5000]))
        paragraph = paragraph[5000:]

    return text_list[:25]


def comprehend():
    s3 = boto3.client("s3")
    bucket_name = "comments-and-titles"
    key = "blob/blob.txt"
    file = s3.get_object(Bucket=bucket_name,
                         Key=key)
    paragraph = str(file['Body'].read())

    comprehend = boto3.client("comprehend")
    sentiment = comprehend.batch_detect_sentiment(TextList=data_chunk(paragraph), LanguageCode="en")



    return sentiment


def break_sentiment(sentiment_dictionary):
    """
    Breaks up the sentiment result json object to make the results easier to read
    :param sentiment_dictionary:
    :return: negative, mixed, neutral, positive
    """
    ResultList = list(sentiment_dictionary.values())[0]
    sentiment_list = []

    for dictionary in ResultList:
        sentiment_list.append(list(dictionary.values())[1])

    negative = 0
    mixed = 0
    neutral = 0
    positive = 0
    for sentiment in sentiment_list:
        if sentiment == "NEGATIVE":
            negative = negative + 1
        if sentiment == "MIXED":
            mixed = mixed + 1
        if sentiment == "NEUTRAL":
            neutral = neutral + 1
        if sentiment == "POSITIVE":
            positive = positive + 1

    return negative, mixed, neutral, positive


if __name__ == '__main__':
    sd = comprehend()[0]
    negative, mixed, neutral, positive = break_sentiment(sd)
