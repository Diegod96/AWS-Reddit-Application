import boto3
import pandas as pd
from textblob import TextBlob
from nltk.sentiment import SentimentIntensityAnalyzer


def get_data():
    # Retrieves txt files from S3 bucket

    # Retrieving comments.txt
    print("Retrieving comments.txt...")
    s3 = boto3.client("s3")
    bucket_name = "comments-and-titles"
    key = "comments/comments.txt"
    file = s3.get_object(Bucket=bucket_name,
                         Key=key)
    comments = str(file["Body"].read())
    print("Retrieved comments.txt!")

    # Retrieving titles.txt
    print("Retrieving titles.txt")
    s3 = boto3.client("s3")
    bucket_name = "comments-and-titles"
    key = "titles/titles.txt"
    file = s3.get_object(Bucket=bucket_name,
                         Key=key)
    titles = str(file["Body"].read())
    print("Retrieved titles.txt!")

    return comments, titles



def clean_latin_comments(comments):

    LATIN_1_CHARS = (
        ('\xe2\x80\x99', "'"),
        ('\xc3\xa9', 'e'),
        ('\xe2\x80\x90', '-'),
        ('\xe2\x80\x91', '-'),
        ('\xe2\x80\x92', '-'),
        ('\xe2\x80\x93', '-'),
        ('\xe2\x80\x94', '-'),
        ('\xe2\x80\x94', '-'),
        ('\xe2\x80\x98', "'"),
        ('\xe2\x80\x9b', "'"),
        ('\xe2\x80\x9c', '"'),
        ('\xe2\x80\x9c', '"'),
        ('\xe2\x80\x9d', '"'),
        ('\xe2\x80\x9e', '"'),
        ('\xe2\x80\x9f', '"'),
        ('\xe2\x80\xa6', '...'),
        ('\xe2\x80\xb2', "'"),
        ('\xe2\x80\xb3', "'"),
        ('\xe2\x80\xb4', "'"),
        ('\xe2\x80\xb5', "'"),
        ('\xe2\x80\xb6', "'"),
        ('\xe2\x80\xb7', "'"),
        ('\xe2\x81\xba', "+"),
        ('\xe2\x81\xbb', "-"),
        ('\xe2\x81\xbc', "="),
        ('\xe2\x81\xbd', "("),
        ('\xe2\x81\xbe', ")")
    )

    try:
        return comments.encode('utf-8')
    except UnicodeDecodeError:
        comments = comments.decode('iso-8859-1')
        for _hex, _char in LATIN_1_CHARS:
            comments = comments.replace(_hex, _char)
        return comments.encode('utf8')


def clean_latin_titles(titles):
    LATIN_1_CHARS = (
        ('\xe2\x80\x99', "'"),
        ('\xc3\xa9', 'e'),
        ('\xe2\x80\x90', '-'),
        ('\xe2\x80\x91', '-'),
        ('\xe2\x80\x92', '-'),
        ('\xe2\x80\x93', '-'),
        ('\xe2\x80\x94', '-'),
        ('\xe2\x80\x94', '-'),
        ('\xe2\x80\x98', "'"),
        ('\xe2\x80\x9b', "'"),
        ('\xe2\x80\x9c', '"'),
        ('\xe2\x80\x9c', '"'),
        ('\xe2\x80\x9d', '"'),
        ('\xe2\x80\x9e', '"'),
        ('\xe2\x80\x9f', '"'),
        ('\xe2\x80\xa6', '...'),
        ('\xe2\x80\xb2', "'"),
        ('\xe2\x80\xb3', "'"),
        ('\xe2\x80\xb4', "'"),
        ('\xe2\x80\xb5', "'"),
        ('\xe2\x80\xb6', "'"),
        ('\xe2\x80\xb7', "'"),
        ('\xe2\x81\xba', "+"),
        ('\xe2\x81\xbb', "-"),
        ('\xe2\x81\xbc', "="),
        ('\xe2\x81\xbd', "("),
        ('\xe2\x81\xbe', ")")
    )

    try:
        return titles.encode('utf-8')
    except UnicodeDecodeError:
        titles = titles.decode('iso-8859-1')
        for _hex, _char in LATIN_1_CHARS:
            titles = titles.replace(_hex, _char)
        return titles.encode('utf8')



def preprocess(comments, titles):


    comments = comments.decode("utf-8")
    comments = comments[2:]

    titles = titles.decode("utf-8")
    titles = titles[2:]

    comments_list = comments.split("\\n")
    titles_list = titles.split("\\n")


    comments_df = pd.DataFrame(comments_list, columns=["Comments"])
    titles_df = pd.DataFrame(titles_list, columns=["Titles"])

    comments_df["polarity"] = comments_df["Comments"].map(lambda text: TextBlob(text).sentiment.polarity)
    print('5 random reviews with the highest positive sentiment polarity: \n')
    cl = comments_df.loc[comments_df.polarity == 1, ["Comments"]].sample(5).values
    for c in cl:
        print(c[0])



if __name__ == '__main__':

    dirty_comments = get_data()[0]
    dirty_titles = get_data()[1]

    comments = clean_latin_comments(dirty_comments)
    titles = clean_latin_titles(dirty_titles)
    
    preprocess(comments, titles)
    


