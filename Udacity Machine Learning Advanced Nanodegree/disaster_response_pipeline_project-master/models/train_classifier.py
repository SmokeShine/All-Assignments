import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.model_selection import GridSearchCV
import nltk
from sklearn.multiclass import OneVsRestClassifier
from sklearn.pipeline import Pipeline
from sklearn.linear_model import SGDClassifier
from nltk.stem import PorterStemmer, WordNetLemmatizer
from nltk.corpus import stopwords 
from nltk import word_tokenize
import re
import matplotlib.pyplot as plt
from matplotlib.pyplot import *
import seaborn as sns
import tqdm
import pdb
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.metrics import roc_auc_score
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import classification_report
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score
from sklearn.naive_bayes import MultinomialNB
from sklearn.multioutput import MultiOutputClassifier
from sklearn.metrics import confusion_matrix

nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
pd.set_option('display.max_colwidth', 0)

stemmer = PorterStemmer()
lemmatiser = WordNetLemmatizer()
cachedStopWords = stopwords.words("english")

def load_data(database_filepath):
	'''
	Input: FilePath of the Database
	Returns: Dataframe splitted into message and their tags
	'''
    engine = create_engine('sqlite:///'+database_filepath)
    name_of_the_table='YourTableName'
    df = pd.read_sql_table(name_of_the_table,engine)
    X = df[['message']]
    Y = df.drop(['id','message','original','genre'],axis=1)
    categories = list(Y.columns.values)
    return X,Y,categories,df

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


def build_model():
	'''
	Returns: pipeline with customer tokenizer and multi output classifier
	'''
    pipeline = Pipeline([
                ('tfidf', TfidfVectorizer(tokenizer=tokenize)),
                ('clf', MultiOutputClassifier(
                                              RandomForestClassifier
                                              (n_estimators=100, random_state=1), n_jobs=-1)),
            ])
    return pipeline


def evaluate_model(model, X_test, X_train, category_names,train,test):
	'''
	Input: Model along with with test, train and categories dataframes
	Returns: Tuned Model
	'''
    print('... Processing ')    
    categories=category_names
    # compute the testing accuracy
    prediction = model.predict(X_test)
    predictionTrain = model.predict(X_train)
	#printing accuracy score for each categories  - For rubric "The accuracy, precision and recall for the test set is outputted for each category."
    i=0
    for category in categories:
        print('Test accuracy for category - {} is {}'.format(category,accuracy_score(test[category].values, prediction[:,i])))
        print("Classification Report")
        print(classification_report(test[category], prediction[:,i]))
        print("Confusion Matrix")
        print(confusion_matrix(test[category], prediction[:,1]))
        print("***"*30)
        i=i+1
    print('Train accuracy is {}'.format(accuracy_score(train[categories].values.flatten(), predictionTrain.flatten())))
    print('Test accuracy is {}'.format(accuracy_score(test[categories].values.flatten(), prediction.flatten())))
    print("Classification Report for Train")
    print(classification_report(train[categories].values.flatten(), predictionTrain.flatten()))
    print("Classification Report for Test")
    print(classification_report(test[categories].values.flatten(), prediction.flatten()))
    print("Train Confusion Matrix")
    print(confusion_matrix(train[categories].values.flatten(), predictionTrain.flatten()))
    print("Test Confusion Matrix")
    print(confusion_matrix(test[categories].values.flatten(), prediction.flatten()))
    parameters = {
        'clf__estimator__criterion': ['gini', 'entropy'],
        'clf__estimator__n_estimators': [50, 100],
    }
    grid_search_tune = GridSearchCV(model, parameters, cv=2, n_jobs=-1, verbose=3)
    #pdb.set_trace()                                
    grid_search_tune.fit(X_train, train[categories])
    print("Best parameters set ")
    print(grid_search_tune.best_estimator_.steps)
    best_parameters = grid_search_tune.best_estimator_.get_params()
    for param_name in sorted(parameters.keys()):
        print ('\t%s: %r' % (param_name, best_parameters[param_name]))
    print("Checking Prediction for Tuned Model")    
    prediction = grid_search_tune.predict(X_test)
    print('Train accuracy is {}'.format(accuracy_score(train[categories].values.flatten(), predictionTrain.flatten())))    
    print("Classification Report for Test")
    print(classification_report(test[categories].values.flatten(), prediction.flatten()))
    print("Test Confusion Matrix")
    print(confusion_matrix(test[categories].values.flatten(), prediction.flatten()))
	#printing accuracy score for each categories  - For rubric "The accuracy, precision and recall for the test set is outputted for each category."
    i=0
    for category in categories:
        print('Test accuracy for category - {} is {}'.format(category,accuracy_score(test[category].values, prediction[:,i])))
        print("Classification Report")
        print(classification_report(test[category], prediction[:,i]))
        print("Confusion Matrix")
        print(confusion_matrix(test[category], prediction[:,1]))
        print("***"*30)
        i=i+1

    return grid_search_tune

def save_model(model, model_filepath):
	'''
	Saving the model to pickle
	'''
    pickle.dump(model, open(model_filepath, 'wb'))


def main():
    if len(sys.argv) == 3:
        database_filepath, model_filepath = sys.argv[1:]
        print('Loading data...\n    DATABASE: {}'.format(database_filepath))
        X, Y, category_names,df = load_data(database_filepath)
        #X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2)
        train, test = train_test_split(df, random_state=42, test_size=0.2, shuffle=True)
        X_train = train.message
        X_test = test.message
        print('Building model...')
        model = build_model()
        
        print('Training model...')
        model.fit(X_train, train[category_names])

        
        print('Evaluating model...')
        evaluate_model(model, X_test, X_train, category_names,train,test)

        print('Saving model...\n    MODEL: {}'.format(model_filepath))
        save_model(model, model_filepath)

        print('Trained model saved!')

    else:
        print('Please provide the filepath of the disaster messages database '\
              'as the first argument and the filepath of the pickle file to '\
              'save the model to as the second argument. \n\nExample: python '\
              'train_classifier.py ../data/DisasterResponse.db classifier.pkl')


if __name__ == '__main__':
    main()