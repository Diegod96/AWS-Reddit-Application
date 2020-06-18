
import re
from dynamo import get_items
from string import punctuation
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.probability import FreqDist
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

articles = get_items()


def parse_table():
    # Parses the table into two lists, articles and comments
    articles_table = articles.scan()
    values_view = articles_table.values()
    value_iterator = iter(values_view)
    items = next(value_iterator)

    # Iterate through the items and assign the value of the comments and title key to their respected lists
    comments = []
    titles = []
    for item in items:
        comments.append(item.get("comments"))
        titles.append(item.get("title"))

    # Flatten the comments list
    comments = [val for sublist in comments for val in sublist]

    return comments, titles


def clean_comments(comments):
    # Cleans comments from any hyperlinks, weird special characters, emojis, etc.
    print("Cleaning comments...")
    clean_comments = []
    for comment in comments:
        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                   u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                   u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                   u"\U00002500-\U00002BEF"  # chinese char
                                   u"\U00002702-\U000027B0"
                                   u"\U00002702-\U000027B0"
                                   u"\U000024C2-\U0001F251"
                                   u"\U0001f926-\U0001f937"
                                   u"\U00010000-\U0010ffff"
                                   u"\u2640-\u2642"
                                   u"\u2600-\u2B55"
                                   u"\u200d"
                                   u"\u23cf"
                                   u"\u23e9"
                                   u"\u231a"
                                   u"\ufe0f"  # dingbats
                                   u"\u3030"
                                   "]+", flags=re.UNICODE)

        comment = comment.replace("\n", "")
        comment = comment.replace("  ", "")
        comment = re.sub(r'^https?:\/\/.*[\r\n]*', '', comment, flags=re.MULTILINE)
        comment = ''.join(i for i in comment if not i in punctuation)
        comment = emoji_pattern.sub(r'', comment)
        _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
        comment = _RE_COMBINE_WHITESPACE.sub(" ", comment).strip()

        clean_comments.append(comment)

    return clean_comments


def clean_titles(titles):
    # Cleans titles from any hyperlinks, weird special characters, emojis, etc.
    print("Cleaning titles...")
    clean_titles = []
    for title in titles:
        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                   u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                   u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                   u"\U00002500-\U00002BEF"  # chinese char
                                   u"\U00002702-\U000027B0"
                                   u"\U00002702-\U000027B0"
                                   u"\U000024C2-\U0001F251"
                                   u"\U0001f926-\U0001f937"
                                   u"\U00010000-\U0010ffff"
                                   u"\u2640-\u2642"
                                   u"\u2600-\u2B55"
                                   u"\u200d"
                                   u"\u23cf"
                                   u"\u23e9"
                                   u"\u231a"
                                   u"\ufe0f"  # dingbats
                                   u"\u3030"
                                   "]+", flags=re.UNICODE)

        title = title.replace("\n", "")
        title = title.replace("  ", "")
        title = re.sub(r'^https?:\/\/.*[\r\n]*', '', title, flags=re.MULTILINE)
        title = ''.join(i for i in title if not i in punctuation)
        title = emoji_pattern.sub(r'', title)
        _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
        title = _RE_COMBINE_WHITESPACE.sub(" ", title).strip()

        clean_titles.append(title)

    return clean_titles


def preprocess(data):
    data = [each_string.lower() for each_string in data]
    content = ', '.join(data)
    content = content.replace(",", "")
    return content


def sentiment_analysis(content):
    analyzer = SentimentIntensityAnalyzer()
    vs = analyzer.polarity_scores(content)


    sentiments = []

    for value in vs.values():
        sentiments.append(value)

    negative = int((sentiments[0])*100)
    neutral = int((sentiments[1])*100)
    positive = int((sentiments[2])*100)

    return negative, neutral, positive


def entities(content):
    tokenized_word = word_tokenize(content)
    stop_words = set(stopwords.words("english"))
    filtered_sent = []
    for w in tokenized_word:
        if w not in stop_words:
            filtered_sent.append(w)
    fdist = FreqDist(filtered_sent)
    fd = pd.DataFrame(fdist.most_common(10), columns=["Word", "Frequency"]).drop([0]).reindex()

    return fd


def dashboard(negative, neutral, positive, fd):
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "xy"}, {"type": "xy"}]],
        subplot_titles=("r/wallstreetbets Frequency Distribution", "r/wallstreetbets Sentiment Distribution")
    )


    fig.add_trace(go.Bar(x=fd["Word"], y=fd["Frequency"]), row=1, col=1)
    fig.add_trace(go.Bar(x=["Negative", "Neutral", "Positive"], y=[negative, neutral, positive]), row=1, col=2)

    fig.update_layout(showlegend=False, title_text="r/wallstreetbets Data Analysis Dashboard")
    fig.show()



if __name__ == '__main__':

    dirty_comments = parse_table()[0]
    dirty_titles = parse_table()[1]
    data = clean_comments(dirty_comments) + clean_titles(dirty_titles)
    content = preprocess(data)
    negative, neutral, positive = sentiment_analysis(content)
    fd = entities(content)
    dashboard(negative, neutral, positive, fd)

