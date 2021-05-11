import boto3
import pandas as pd
import time

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    s3_input_file = 'output/YouTube_Statistics_Data/YouTube_Statistics_Table'+time.strftime("%Y_%m_%d") + '.csv'
    bucket_name = 'thehoxtontrend' 
    resp = s3_client.get_object(Bucket=bucket_name, Key=s3_input_file)

    data = pd.read_csv(resp['Body'], sep=';')
    
    data_list = data[["views", "comments"]].values.tolist()
    
    # Simple Linear Regression on the Swedish Insurance Dataset
    from random import seed
    from random import randrange
    from math import sqrt
    
    # Split a dataset into a train and test set
    def train_test_split(dataset, split):
    	train = list()
    	train_size = split * len(dataset)
    	dataset_copy = list(dataset)
    	while len(train) < train_size:
    		index = randrange(len(dataset_copy))
    		train.append(dataset_copy.pop(index))
    	return train, dataset_copy
     
    # Calculate root mean squared error
    def rmse_metric(actual, predicted):
    	sum_error = 0.0
    	for i in range(len(actual)):
    		prediction_error = predicted[i] - actual[i]
    		sum_error += (prediction_error ** 2)
    	mean_error = sum_error / float(len(actual))
    	return sqrt(mean_error)
     
    # Evaluate an algorithm using a train/test split
    def evaluate_algorithm(dataset, algorithm, split, *args):
    	train, test = train_test_split(dataset, split)
    	test_set = list()
    	for row in test:
    		row_copy = list(row)
    		row_copy[-1] = None
    		test_set.append(row_copy)
    	predicted = algorithm(train, test_set, *args)
    	actual = [row[-1] for row in test]
    	rmse = rmse_metric(actual, predicted)
    	return rmse
     
    # Calculate the mean value of a list of numbers
    def mean(values):
    	return sum(values) / float(len(values))
     
    # Calculate covariance between x and y
    def covariance(x, mean_x, y, mean_y):
    	covar = 0.0
    	for i in range(len(x)):
    		covar += (x[i] - mean_x) * (y[i] - mean_y)
    	return covar
     
    # Calculate the variance of a list of numbers
    def variance(values, mean):
    	return sum([(x-mean)**2 for x in values])
     
    # Calculate coefficients
    def coefficients(dataset):
    	x = [row[0] for row in dataset]
    	y = [row[1] for row in dataset]
    	x_mean, y_mean = mean(x), mean(y)
    	b1 = covariance(x, x_mean, y, y_mean) / variance(x, x_mean)
    	b0 = y_mean - b1 * x_mean
    	return [b0, b1]
     
    # Simple linear regression algorithm
    def simple_linear_regression(train, test):
    	predictions = list()
    	b0, b1 = coefficients(train)
    	for row in test:
    		yhat = b0 + b1 * row[0]
    		predictions.append(yhat)
    	return predictions
    
    # Simple linear regression on insurance dataset
    seed(1)
    
    # evaluate algorithm
    split = 0.6
    rmse = evaluate_algorithm(data_list, simple_linear_regression, split)
    model_coefficients = coefficients(data_list)
    
    import json
    
    # Putting results into json format
    model_results = {"RMSE" : [rmse],
                "Model Coefficients" : model_coefficients}
    json_model_results = json.dumps(model_results)
    
    # Constructing http response object
    responseObject = {}
    responseObject['statusCode'] = 200
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['body'] = json_model_results
    
    # Return the response object
    return responseObject