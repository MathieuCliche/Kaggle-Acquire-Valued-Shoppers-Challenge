Python code for the kaggle competition : Acquire valued shoppers challenge.

The data was loaded in a MySQL database.  The main table which contains the transaction history of all custummers over a 1 year period had the following schema :

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



ALTER TABLE transactions ADD PRIMARY INDEX (id);
