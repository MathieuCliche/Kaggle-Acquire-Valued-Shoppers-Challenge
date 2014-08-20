Python code for the kaggle competition : Acquire valued shoppers challenge -> http://www.kaggle.com/c/acquire-valued-shoppers-challenge

The data was loaded in a MySQL database.  For example, the main table which contains the transaction history of all custummers over a 1 year period had the following schema :

CREATE TABLE transactions
(

  id      char(50)  NOT NULL ,

  chain_    int  NOT NULL ,

  dept int NOT NULL,
 
 category int NOT NULL,
 
 company char(50) NOT NULL,
 
 brand char(10) NOT NULL,
  
date_ date NOT NULL,
 
 productsize int NOT NULL,
 
 productmeasure char(10) NOT NULL,
 
 purchasequantity int NOT NULL,
 
 purchaseamount float NOT NULL
);



ALTER TABLE transactions ADD INDEX (date_);
ALTER TABLE transactions ADD INDEX (category);
ALTER TABLE transactions ADD INDEX (company);
ALTER TABLE transactions ADD INDEX (brand);

LOAD DATA LOCAL INFILE 'C:/Users/Mathieu/Documents/shopper/transactions.csv' 

INTO TABLE transactions
FIELDS TERMINATED BY ',' 

LINES TERMINATED BY '\n' 

IGNORE 1 LINES;

Then features were then generated with the featureeng.py python code.  All features were generated via SQL queries using the python librairy MySQLdb which can access the MySQL database.  The features were then saved in a numpy array file.

The model is then trained and tested in training.py.  In this python code, the custummers are first sorted with respect of the offer date so that the cross-validation respects the time structure of this problem.  Then the features are scaled and each sample is weighted with respect to the number of repeat trips the custummer did.  After this preprocessing step a random-forest model is trained and cross-validated with the ROC AUC metric.  Finally we predict the probability of repeat trips for the custummers in the test set and make a csv file which can be submitted to Kaggle.  