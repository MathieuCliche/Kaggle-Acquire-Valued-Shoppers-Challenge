import csv
from sklearn.metrics import roc_auc_score
from math import *
import numpy as np
from sklearn.ensemble import RandomForestClassifier

trainnb=0.7 

print 'Pickling out'

trainout=np.load('trainout.npy')
featuretr=np.load('datatrain19merged2.npy')
numbertr=len(featuretr)

featurete=np.load('datatest19merged2.npy')
numberte=len(featurete)

ids=np.load('dataid.npy')

## SORT BY DATE
orderdate=featuretr[0::,-1].argsort()
trainout=trainout[orderdate]
featuretr=featuretr[orderdate,0::]

print 'Feature scaling'
standarddev=[]
mean=[]
for k in range(len(featuretr[0,0::])):
    standarddev.append(np.std(featuretr[0::,k]))
    if standarddev[k]==0:
        standarddev[k]=1
    mean.append(np.mean(featuretr[0::,k]))
    featuretr[0::,k]=np.subtract(featuretr[0::,k],mean[k])/standarddev[k]
    featurete[0::,k]=np.subtract(featurete[0::,k],mean[k])/standarddev[k]
    #CAPPING
    capfactor=1.0e2
    featuretr[0::,k]=capfactor*np.tanh(featuretr[0::,k]/capfactor)
    featurete[0::,k]=capfactor*np.tanh(featurete[0::,k]/capfactor)
    

oldbinaryout=(trainout>0)

valid_data=featuretr[round(trainnb*numbertr):numbertr,0::]
valid_data_out=trainout[round(trainnb*numbertr):numbertr]
valid_data_outb=oldbinaryout[round(trainnb*numbertr):numbertr]
train_data=featuretr[0:round(trainnb*numbertr),0::]
train_data_out=trainout[0:round(trainnb*numbertr)]
train_data_outb=oldbinaryout[0:round(trainnb*numbertr)]

# ADD REPEAT TRIPS  (JUST FOR TRAINING)
mint=5.0
maxt=5000.0
weights=np.ones(np.shape(train_data_out))+(train_data_out>=mint).astype(float)*(maxt*np.tanh(train_data_out/maxt))

print 'Training '

rf_class=RandomForestClassifier(n_estimators = 500,n_jobs=8, random_state = 987654321, verbose=1,oob_score =True)
rf_class.fit(train_data,np.ravel(train_data_outb),sample_weight=np.ravel(weights))

print 'Validation'

if trainnb==1:
    pass
else:
    
    output_validb = rf_class.predict_proba(valid_data)[0::,1]
    
    valid_data_outb=np.ravel(valid_data_outb)
    output_validb=np.ravel(output_validb)
    
    logerror=-np.average((valid_data_outb)*np.log(output_validb+1e-10)+(1.0-valid_data_outb)*np.log(1.0-output_validb+1e-10))
    roc=roc_auc_score(valid_data_outb, output_validb)
    sqrerror=np.average((output_validb-valid_data_outb)**2)
    
    print "logerror", logerror
    print "Sqrerror", sqrerror
    print "roc auc", roc


print 'Predicting'

predictions=rf_class.predict_proba(featurete)[0::,1]

file_object=open("Shop_RF.csv", "wb")
open_file_object = csv.writer(file_object)
open_file_object.writerow(["id","repeatProbability"])
open_file_object.writerows(zip(ids.astype(long).flatten(), predictions.flatten()))
file_object.close()