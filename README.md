# Credit Card Fraud Detection Using PySpark

## Overview
This project is dedicated to the development and evaluation of machine learning models aimed at detecting fraudulent credit card transactions. Utilizing the distributed computing power of PySpark, the project processes large datasets efficiently to build models that can identify potentially fraudulent activities with high accuracy. Each model implemented achieves an impressive accuracy of 97%.

## Project Aim
The primary objective of this project is to apply advanced machine learning techniques in PySpark to effectively identify and predict fraudulent transactions in credit card data. By leveraging PySpark's capabilities, the project ensures scalable data handling and processing, which is essential for dealing with the volume and velocity of financial transaction data.

## Models
The models used in this project include:
1. Gradient Boosted Trees
2. Random Forest Classifier
3. Logistic Regression

These models were chosen for their robustness and effectiveness in handling imbalanced datasets typical of fraud detection scenarios.

## Data Preparation
The dataset was prepared using PySpark, which involved:
- Cleaning the data to ensure quality and consistency.
- Engineering relevant features that are indicative of fraudulent behaviors.
- Splitting the data into training and testing sets to ensure a fair evaluation of model performance.

## Feature Engineering
Critical features were engineered to enhance the models' ability to discern fraudulent transactions, including:
- Label encoding of categorical variables.
- Normalizing and scaling of numerical inputs.
- Extracting time-based features that may signal unusual transaction activity.

## Training
The models were trained within the Spark MLlib framework. Detailed steps including parameter tuning and model optimization are documented in the training scripts.

## Evaluation
Model performance was evaluated using several metrics:
- Accuracy: Each model achieved approximately 97% accuracy.
- Confusion Matrix: Helps visualize the performance of the models beyond accuracy.
- Classification Report: Provides a breakdown of precision, recall, and F1-score.
- AUC-ROC: Measures the ability of the model to distinguish between classes.

## Usage
To use these models for fraud detection:
1. Set up a Spark environment capable of handling large datasets.
2. Load the training and evaluation scripts provided.
3. Execute the training script to build the model.
4. Run the evaluation script to assess the model's performance.

## Requirements
- Apache Spark 3.0 or above
- Python 3.6 or above
- Required Python libraries include pyspark, pandas, and numpy.

## Conclusion
The machine learning models developed in this project provide a reliable and scalable solution for detecting fraudulent transactions in credit card data. They are well-suited for further deployment in real-time transaction processing systems, offering a significant tool in the fight against financial fraud.
