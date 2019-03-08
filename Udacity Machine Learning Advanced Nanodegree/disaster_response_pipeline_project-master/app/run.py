import json
import plotly
import pandas as pd
import numpy as np
import pdb
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
import plotly
from flask import Flask
from flask import render_template, request, jsonify
from plotly.graph_objs import Bar,Pie,Scatter,Histogram
from sklearn.externals import joblib
from sqlalchemy import create_engine
from wordcloud import WordCloud, STOPWORDS 
import random
from collections import Counter

import re
import nltk
from nltk.stem import PorterStemmer, WordNetLemmatizer
from nltk.corpus import stopwords 
from nltk import word_tokenize
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

stemmer = PorterStemmer()
lemmatiser = WordNetLemmatizer()
cachedStopWords = stopwords.words("english")

app = Flask(__name__)

def tokenize(text):
    '''
	Input: Text 
	Returns: Tokenized Text,post usage of lemmatiser
	'''
    posts = text
    temp = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', 'link', posts)
    temp = re.sub("[^a-zA-Z]", " ", temp)
    temp = re.sub(' +', ' ', temp).lower()
    temp = " ".join([lemmatiser.lemmatize(w) for w in temp.split(' ') if w not in cachedStopWords])
    temp=nltk.word_tokenize(temp)
    return temp
# load data
engine = create_engine('sqlite:///../data/DisasterResponse.db')
df = pd.read_sql_table('YourTableName', engine)
#pdb.set_trace()
# load model
model = joblib.load("../models/classifier.pkl")


# index webpage displays cool visuals and receives user input text for model
@app.route('/')
@app.route('/index')
def index():
    
    # extract data needed for visuals
    # TODO: Below is an example - modify to extract data for your own visuals
    genre_counts = df.groupby('genre').count()['message']
    #pdb.set_trace()
    genre_names = list(genre_counts.index)
    
    complete_list = ' '.join([i for i in df['message']])
    counts = Counter(tokenize(complete_list))
    words= [word for word, word_count in Counter(counts).most_common(30)]
    colors = [plotly.colors.DEFAULT_PLOTLY_COLORS[random.randrange(1, 10)] for i in range(30)]
    weights = list(range(30))[::-1]
    
    # create visuals
    # TODO: Below is an example - modify to create your own visuals
    graphs = [
        {
            'data': [
                Bar(
                    x=genre_names,
                    y=genre_counts
                )
            ],

            'layout': {
                'title': 'Distribution of Message Genres',
                'yaxis': {
                    'title': "Count"
                },
                'xaxis': {
                    'title': "Genre"
                }
            }
        },
                {
            'data': [
                Pie(
                    labels=genre_names,
                    values=genre_counts,
                    pull=.2,
                    hole=.2,
                    #colorway='blues',
                    textposition='outside',
                    textinfo='percent'
                )
            ],

            'layout': {
                'title': 'Pie Chart of Message Genres'
            }
        },
        {
            'data': [
                Scatter(
                    x=[random.random() for i in range(30)],
                    y=list(range(30)),
                    mode='text',
                    text=words,
                    marker={'opacity': 0.3},
                    textfont={'color': colors}
                )
            ],

            'layout': {
                'title': 'WordCloud - Top Words',
                'xaxis': {'showgrid': False, 'showticklabels': False, 'zeroline': False},
                'yaxis': {'showgrid': False, 'showticklabels': False, 'zeroline': False},
                'hoverinfo':'none'
            }
        }
    ]
    

    # encode plotly graphs in JSON
    ids = ["graph-{}".format(i) for i, _ in enumerate(graphs)]
    graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)
    
    # render web page with plotly graphs
    return render_template('master.html', ids=ids, graphJSON=graphJSON)


# web page that handles user query and displays model results
@app.route('/go')
def go():
    # save user input in query
    query = request.args.get('query', '') 

    # use model to predict classification for query
    #pdb.set_trace()
    classification_labels = model.predict([query])[0]
    #classification_labels = model.predict([query])
    classification_results = dict(zip(df.columns[4:], classification_labels))

    # This will render the go.html Please see that file. 
    return render_template(
        'go.html',
        query=query,
        classification_result=classification_results
    )


def main():
    app.run(host='0.0.0.0', port=3001, debug=True)


if __name__ == '__main__':
    main()