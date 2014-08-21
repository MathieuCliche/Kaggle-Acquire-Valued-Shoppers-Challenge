"""
This python script is used to generate the features which will later be used to train the classifier.
Example of features :
-  Feature describing the offer (offervalue, date the offer was made, etc.)
-  Features describing the customer (how much (money, number of trips, quantity) was spent in total by the customer).
-  Feature describing the relation between the customer and the offer (amount of money spend, number of times purchased,
quantity purchased in the same (company, category, brand) as their respective offer for (10, 30, 60, 90, 120, 180 , 365)
days after the offer was made.  

All features are obtained by quering the database via the MySQLdb librairy.  The matrix representing the features is
save in a npy file at the end.
"""

import MySQLdb
import numpy as np
from datetime import datetime, date

test=1 #Extract feature for train (test=0) or test (test=1).

db = MySQLdb.connect(host="localhost", # your host, usually localhost
                     user="root", # your username
                     passwd="password", #your password
                      db="shopper")

cur = db.cursor()

sourcetrans='transcat2' # reduced transactions
#(only contains relevant companies and categories, used for queries which don't need the entire customer history)
sourcetranslong='transactions' #full transactions
#(this one contains the full transaction history, used for queries for features that describes the customer)

if test:
    source='testHistory'
    
    ### IDS TEST ####
    cur.execute("select id\
                from "+source+\
                " order by CAST(id AS UNSIGNED)")
    ids=[]
    for row in cur.fetchall() :
        ids.append(row)
    ids=np.array(ids)
    np.save("dataid",ids)
    
    string='data'+'test23'
else:
    source='trainHistory'
    
    ### TRAIN OUTPUT ####
    cur.execute("select repeattrips\
                from "+source+\
                " order by CAST(id AS UNSIGNED)")
    trainout=[]
    for row in cur.fetchall() :
        trainout.append(row)
    trainout=np.array(trainout)
    
    string='data'+'train23'
    np.save('trainout',trainout)

start = datetime.now()

print datetime.now() - start

#### PURCHASE AMOUNT, REPEAT TRIP, QUANTITY FROM THE SAME COMPANY AS THE OFFER OF THE CUSTOMER#
comp=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch,sum(purchasequantity) sumqte \
            from " + source + " , (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.company=offers.company \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    comp.append(row)
comp=np.nan_to_num(np.array(comp,dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT TRIP FROM THE SAME COMPANY PAST 10 DAYS#
comp10=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch,sum(purchasequantity) sumqte \
            from " + source + " , (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.company=offers.company  and ((date_)+INTERVAL 10 DAY)>(offerdate)\
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    comp10.append(row)
comp10=np.nan_to_num(np.array(comp10,dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT TRIP FROM THE SAME COMPANY PAST 30 DAYS#
comp30=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch,sum(purchasequantity) sumqte \
            from " + source + " , (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.company=offers.company  and ((date_)+INTERVAL 30 DAY)>(offerdate)\
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    comp30.append(row)
comp30=np.nan_to_num(np.array(comp30,dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT TRIP FROM THE SAME COMPANY PAST 60 DAYS#
comp60=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch,sum(purchasequantity) sumqte \
            from " + source + " , (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.company=offers.company  and ((date_)+INTERVAL 60 DAY)>(offerdate)\
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    comp60.append(row)
comp60=np.nan_to_num(np.array(comp60,dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT TRIP FROM THE SAME COMPANY PAST 90 DAYS#
comp90=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch,sum(purchasequantity) sumqte \
            from " + source + " , (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.company=offers.company  and ((date_)+INTERVAL 90 DAY)>(offerdate)\
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    comp90.append(row)
comp90=np.nan_to_num(np.array(comp90,dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT TRIP FROM THE SAME COMPANY PAST 120 DAYS#
comp120=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch,sum(purchasequantity) sumqte \
            from " + source + " , (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.company=offers.company  and ((date_)+INTERVAL 120 DAY)>(offerdate)\
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    comp120.append(row)
comp120=np.nan_to_num(np.array(comp120,dtype=np.float))


print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT TRIP FROM THE SAME COMPANY PAST 180 DAYS#
comp180=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch,sum(purchasequantity) sumqte \
            from " + source + " , (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.company=offers.company  and ((date_)+INTERVAL 180 DAY)>(offerdate)\
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    comp180.append(row)
comp180=np.nan_to_num(np.array(comp180,dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT FROM THE SAME BRAND #
brand=[]
cur.execute("select sumpurch, countpurch, sumqte, sumsize\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte, sum(productsize) sumsize \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.brand=offers.brand and tcat.company=offers.company and tcat.category=offers.category \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    brand.append(row)
brand=np.nan_to_num(np.array(brand, dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT FROM THE SAME BRAND 7 DAYS#
brand10=[]
cur.execute("select sumpurch, countpurch, sumqte, sumsize\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte, sum(productsize) sumsize \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.brand=offers.brand and tcat.company=offers.company and tcat.category=offers.category and ((date_)+INTERVAL 10 DAY)>(offerdate) \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    brand10.append(row)
brand10=np.nan_to_num(np.array(brand10, dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT FROM THE SAME BRAND 30 DAYS#
brand30=[]
cur.execute("select sumpurch, countpurch, sumqte, sumsize\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte, sum(productsize) sumsize \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.brand=offers.brand and tcat.company=offers.company and tcat.category=offers.category and ((date_)+INTERVAL 30 DAY)>(offerdate) \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    brand30.append(row)
brand30=np.nan_to_num(np.array(brand30, dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT FROM THE SAME BRAND 60 DAYS#
brand60=[]
cur.execute("select sumpurch, countpurch, sumqte, sumsize\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte, sum(productsize) sumsize \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.brand=offers.brand and tcat.company=offers.company and tcat.category=offers.category and ((date_)+INTERVAL 60 DAY)>(offerdate) \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    brand60.append(row)
brand60=np.nan_to_num(np.array(brand60, dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT FROM THE SAME BRAND 60 DAYS#
brand90=[]
cur.execute("select sumpurch, countpurch, sumqte, sumsize\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte, sum(productsize) sumsize \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.brand=offers.brand and tcat.company=offers.company and tcat.category=offers.category and ((date_)+INTERVAL 90 DAY)>(offerdate) \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    brand90.append(row)
brand90=np.nan_to_num(np.array(brand90, dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT FROM THE SAME BRAND 120 DAYS#
brand120=[]
cur.execute("select sumpurch, countpurch, sumqte, sumsize\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte, sum(productsize) sumsize \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.brand=offers.brand and tcat.company=offers.company and tcat.category=offers.category and ((date_)+INTERVAL 120 DAY)>(offerdate) \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    brand120.append(row)
brand120=np.nan_to_num(np.array(brand120, dtype=np.float))


print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT FROM THE SAME BRAND 120 DAYS#
brand180=[]
cur.execute("select sumpurch, countpurch, sumqte, sumsize\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte, sum(productsize) sumsize \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.brand=offers.brand and tcat.company=offers.company and tcat.category=offers.category and ((date_)+INTERVAL 180 DAY)>(offerdate) \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    brand180.append(row)
brand180=np.nan_to_num(np.array(brand180, dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT  FROM THE SAME category #
cat=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.category=offers.category \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    cat.append(row)
cat=np.nan_to_num(np.array(cat, dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT  FROM THE SAME category  7 DAYS#
cat10=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.category=offers.category and ((date_)+INTERVAL 10 DAY)>(offerdate) \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    cat10.append(row)
cat10=np.nan_to_num(np.array(cat10, dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT  FROM THE SAME category  30 DAYS#
cat30=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.category=offers.category and ((date_)+INTERVAL 30 DAY)>(offerdate) \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    cat30.append(row)
cat30=np.nan_to_num(np.array(cat30, dtype=np.float))

print datetime.now() - start


#### PURCHASE AMOUNT, QUANTITY, REPEAT  FROM THE SAME category  60 DAYS#
cat60=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.category=offers.category and ((date_)+INTERVAL 60 DAY)>(offerdate) \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    cat60.append(row)
cat60=np.nan_to_num(np.array(cat60, dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT  FROM THE SAME category  90 DAYS#
cat90=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.category=offers.category and ((date_)+INTERVAL 90 DAY)>(offerdate) \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    cat90.append(row)
cat90=np.nan_to_num(np.array(cat90, dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT  FROM THE SAME category  120 DAYS#
cat120=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.category=offers.category and ((date_)+INTERVAL 120 DAY)>(offerdate) \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    cat120.append(row)
cat120=np.nan_to_num(np.array(cat120, dtype=np.float))

print datetime.now() - start

#### PURCHASE AMOUNT, QUANTITY, REPEAT  FROM THE SAME category  180 DAYS#
cat180=[]
cur.execute("select sumpurch, countpurch, sumqte\
            from " + source + " left outer join (\
            select " + source + ".id, sum(purchaseamount) sumpurch, count(purchaseamount) countpurch, sum(purchasequantity) sumqte \
            from  " + source + ", (select * from "+sourcetrans+") as tcat, offers  \
            where offers.offer=" + source + ".offer and " + source + ".id=tcat.id and tcat.category=offers.category and ((date_)+INTERVAL 180 DAY)>(offerdate) \
            group by tcat.id \
            ) as bigtable on " + source + ".id=bigtable.id \
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    cat180.append(row)
cat180=np.nan_to_num(np.array(cat180, dtype=np.float))

print datetime.now() - start


#### TOTAL MONEY IN CHAIN #### AND QTE AND TRANSACTIONS  ***********************
chaintot=[]
cur.execute("select sumcomp, countpurch, sumqte\
            from "+source+" left outer join (\
            select sum(tcat.purchaseamount) sumcomp, count(tcat.purchaseamount) countpurch, sum(tcat.purchasequantity) sumqte, chain_\
            from (select * from "+sourcetranslong+") as tcat\
            group by tcat.chain_) as sctable on "+source+".chain_=sctable.chain_\
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    chaintot.append(row)
chaintot=np.nan_to_num(np.array(chaintot, dtype=np.float))

print datetime.now() - start

#### TOTAL MONEY IN COMPANY ####
comptot=[]
cur.execute("select sumcomp, countpurch, sumqte\
            from "+source+" left outer join (\
            select offers.offer, sumcomp, countpurch, sumqte\
            from offers left outer join (\
            select sum(tcat.purchaseamount) sumcomp, count(tcat.purchaseamount) countpurch, sum(tcat.purchasequantity) sumqte, tcat.company\
            from (select * from "+sourcetrans+") as tcat\
            group by tcat.company) as sctable on sctable.company=offers.company) as sotable on "+source+".offer=sotable.offer\
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    comptot.append(row)
comptot=np.nan_to_num(np.array(comptot, dtype=np.float))

print datetime.now() - start

#### TOTAL MONEY IN BRAND ####
brandtot=[]
cur.execute("select sumbrand, countpurch, sumqte, sumsize \
            from "+source+" left outer join (\
            select offers.offer, sumbrand, countpurch, sumqte, sumsize\
            from offers left outer join (\
            select sum(tcat.purchaseamount) sumbrand, count(tcat.purchaseamount) countpurch, sum(tcat.purchasequantity) sumqte, sum(tcat.productsize) sumsize , tcat.brand\
            from (select * from "+sourcetrans+") as tcat\
            group by tcat.brand) as sctable on sctable.brand=offers.brand) as sotable on "+source+".offer=sotable.offer\
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    brandtot.append(row)
brandtot=np.nan_to_num(np.array(brandtot, dtype=np.float))

print datetime.now() - start

#### TOTAL MONEY IN CATEGORY ####
cattot=[]
cur.execute("select sumcat, countpurch, sumqte\
            from "+source+" left outer join (\
            select offers.offer, sumcat, countpurch, sumqte\
            from offers left outer join (\
            select sum(tcat.purchaseamount) sumcat, count(tcat.purchaseamount) countpurch, sum(tcat.purchasequantity) sumqte, tcat.category\
            from (select * from "+sourcetrans+") as tcat\
            group by tcat.category) as sctable on sctable.category=offers.category) as sotable on "+source+".offer=sotable.offer\
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    cattot.append(row)
cattot=np.nan_to_num(np.array(cattot, dtype=np.float))

print datetime.now() - start


#### TOTAL PURCHASE AMOUNT FROM USER ########   
purchtot=[]
cur.execute("select sum(tcat.purchaseamount), count(tcat.purchaseamount), sum(tcat.purchasequantity), sum(tcat.productsize)\
            from "+source+" left outer join (select * from "+sourcetranslong+") as tcat on  "+source+".id=tcat.id\
            group by "+source+".id\
            order by CAST("+source+".id AS UNSIGNED);")

for row in cur.fetchall():
    purchtot.append(row)
purchtot=np.nan_to_num(np.array(purchtot, dtype=np.float))
purchtot_brand=purchtot
purchtot=np.hstack((purchtot[0::,0:3],purchtot[0::,4:7]))

print datetime.now() - start

#### TOTAL PURCHASE AMOUNT FROM USER PAST 10 DAYS ########   ***********************
purchtot10=[]
cur.execute("select sum(tcat.purchaseamount), count(tcat.purchaseamount), sum(tcat.purchasequantity), sum(tcat.productsize)\
            from "+source+" left outer join (select * from "+sourcetranslong+") as tcat on  "+source+".id=tcat.id and ((date_)+INTERVAL 10 DAY)>(offerdate)\
            group by "+source+".id\
            order by CAST("+source+".id AS UNSIGNED);")

for row in cur.fetchall():
    purchtot10.append(row)
purchtot10=np.nan_to_num(np.array(purchtot10, dtype=np.float))
purchtot_brand10=purchtot10
purchtot10=np.hstack((purchtot10[0::,0:3],purchtot10[0::,4:7]))

print datetime.now() - start

#### TOTAL PURCHASE AMOUNT FROM USER PAST 30 DAYS ########   ***********************
purchtot30=[]
cur.execute("select sum(tcat.purchaseamount), count(tcat.purchaseamount), sum(tcat.purchasequantity), sum(tcat.productsize)\
           from "+source+" left outer join (select * from "+sourcetranslong+") as tcat on  "+source+".id=tcat.id and ((date_)+INTERVAL 30 DAY)>(offerdate)\
            group by "+source+".id\
            order by CAST("+source+".id AS UNSIGNED);")

for row in cur.fetchall():
    purchtot30.append(row)
purchtot30=np.nan_to_num(np.array(purchtot30, dtype=np.float))
purchtot_brand30=purchtot30
purchtot30=np.hstack((purchtot30[0::,0:3],purchtot30[0::,4:7]))

print datetime.now() - start

#### TOTAL PURCHASE AMOUNT FROM USER PAST 60 DAYS ########   ***********************
purchtot60=[]
cur.execute("select sum(tcat.purchaseamount), count(tcat.purchaseamount), sum(tcat.purchasequantity), sum(tcat.productsize)\
            from "+source+" left outer join (select * from "+sourcetranslong+") as tcat on  "+source+".id=tcat.id and ((date_)+INTERVAL 60 DAY)>(offerdate)\
            group by "+source+".id\
            order by CAST("+source+".id AS UNSIGNED);")

for row in cur.fetchall():
    purchtot60.append(row)
purchtot60=np.nan_to_num(np.array(purchtot60, dtype=np.float))
purchtot_brand60=purchtot60
purchtot60=np.hstack((purchtot60[0::,0:3],purchtot60[0::,4:7]))

print datetime.now() - start


#### TOTAL PURCHASE AMOUNT FROM USER PAST 90 DAYS ########   ***********************
purchtot90=[]
cur.execute("select sum(tcat.purchaseamount), count(tcat.purchaseamount), sum(tcat.purchasequantity), sum(tcat.productsize)\
            from "+source+" left outer join (select * from "+sourcetranslong+") as tcat on  "+source+".id=tcat.id and ((date_)+INTERVAL 90 DAY)>(offerdate)\
            group by "+source+".id\
            order by CAST("+source+".id AS UNSIGNED);")


for row in cur.fetchall():
    purchtot90.append(row)
purchtot90=np.nan_to_num(np.array(purchtot90, dtype=np.float))
purchtot_brand90=purchtot90
purchtot90=np.hstack((purchtot90[0::,0:3],purchtot90[0::,4:7]))


print datetime.now() - start

#### TOTAL PURCHASE AMOUNT FROM USER PAST 120 DAYS ########   ***********************
purchtot120=[]
cur.execute("select sum(tcat.purchaseamount), count(tcat.purchaseamount), sum(tcat.purchasequantity), sum(tcat.productsize)\
            from "+source+" left outer join (select * from "+sourcetranslong+") as tcat on  "+source+".id=tcat.id and ((date_)+INTERVAL 120 DAY)>(offerdate)\
            group by "+source+".id\
            order by CAST("+source+".id AS UNSIGNED);")

for row in cur.fetchall():
    purchtot120.append(row)
purchtot120=np.nan_to_num(np.array(purchtot120, dtype=np.float))
purchtot_brand120=purchtot120
purchtot120=np.hstack((purchtot120[0::,0:3],purchtot120[0::,4:7]))

print datetime.now() - start

#### TOTAL PURCHASE AMOUNT FROM USER PAST 180 DAYS ########   ***********************
purchtot180=[]
cur.execute("select sum(tcat.purchaseamount), count(tcat.purchaseamount), sum(tcat.purchasequantity), sum(tcat.productsize)\
            from "+source+" left outer join (select * from "+sourcetranslong+") as tcat on  "+source+".id=tcat.id and ((date_)+INTERVAL 180 DAY)>(offerdate)\
            group by "+source+".id\
            order by CAST("+source+".id AS UNSIGNED);")

for row in cur.fetchall():
    purchtot180.append(row)
purchtot180=np.nan_to_num(np.array(purchtot180, dtype=np.float))
purchtot_brand180=purchtot180
purchtot180=np.hstack((purchtot180[0::,0:3],purchtot180[0::,4:7]))

print datetime.now() - start

########################################################################################

##### OFFER FEATURES ###
offerfea=[]
cur.execute("select offervalue\
            from " + source + ", offers\
            where " + source + ".offer=offers.offer\
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    offerfea.append(row)
offerfea=np.nan_to_num(np.array(offerfea, dtype=np.float))

print datetime.now() - start

offermarket=[]
cur.execute("select market\
            from " + source + "\
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    offermarket.append(row)
offermarket=np.nan_to_num(np.array(offermarket, dtype=np.float))
offermarktype=np.unique(offermarket)

for j in range(len(offermarktype)):
    offerfea=np.hstack((offerfea,(offermarket==offermarktype[j]).astype(int)))

print datetime.now() - start

##### RELATIVE DATE IN THE WEEK THE OFFER WAS MADE ###
weekdate=[]
cur.execute("select dayofweek((offerdate))\
            from " + source + "\
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    weekdate.append(row)
weekdate=np.nan_to_num(np.array(weekdate, dtype=np.float))
weekdateoriginal=weekdate
weekdate=weekdateoriginal==1
weekendornot=(weekdateoriginal==1).astype(int)+(weekdateoriginal==6).astype(int)+(weekdateoriginal==7).astype(int)

for k in range(6):
    weekdate=np.hstack((weekdate,weekdateoriginal==(k+2)))
    
weekdate=weekdate.astype(int)
weekdate=np.hstack((weekdate,weekendornot))

##### RELATIVE DATE IN THE MONTH THE OFFER WAS MADE ###
monthdate=[]
cur.execute("select dayofmonth((offerdate))\
            from " + source + "\
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    monthdate.append(row)
monthdate=np.nan_to_num(np.array(monthdate, dtype=np.float))
firstweek=(monthdate<=7).astype(int)
secondweek=(np.logical_and(monthdate<=14,monthdate>7)).astype(int)
thirdweek=(np.logical_and(monthdate<=21,monthdate>14)).astype(int)
fourthweek=(np.logical_and(monthdate<=28,monthdate>21)).astype(int)
fifthweek=(monthdate>28).astype(int)
monthdate=np.hstack((firstweek,secondweek,thirdweek,fourthweek,fifthweek))


##### RELATIVE DATE THE OFFER WAS MADE ###
offerdate=[]
cur.execute("select datediff((offerdate),(select min(offerdate) from trainHistory))\
            from " + source + "\
            order by CAST(" + source + ".id AS UNSIGNED)")

for row in cur.fetchall():
    offerdate.append(row)
offerdate=np.nan_to_num(np.array(offerdate, dtype=np.float))


print datetime.now() - start

small=1e-8
size=len(brand)

#FEATURE PACKING

purchtotpurch=np.reshape(purchtot[0::,0],(size,1))
purchtotpurch10=np.reshape(purchtot10[0::,0],(size,1))
purchtotpurch30=np.reshape(purchtot30[0::,0],(size,1))
purchtotpurch60=np.reshape(purchtot60[0::,0],(size,1))
purchtotpurch90=np.reshape(purchtot90[0::,0],(size,1))
purchtotpurch120=np.reshape(purchtot120[0::,0],(size,1))
purchtotpurch180=np.reshape(purchtot180[0::,0],(size,1))
fpurch=np.hstack(((purchtotpurch>0).astype(float),purchtot,purchtot/(chaintot+small)))
fpurch10=np.hstack(((purchtotpurch10>0).astype(float),purchtot10,purchtot10/(chaintot+small)))
fpurch30=np.hstack(((purchtotpurch30>0).astype(float),purchtot30,purchtot30/(chaintot+small)))
fpurch60=np.hstack(((purchtotpurch60>0).astype(float),purchtot60,purchtot60/(chaintot+small)))
fpurch90=np.hstack(((purchtotpurch90>0).astype(float),purchtot90,purchtot90/(chaintot+small)))
fpurch120=np.hstack(((purchtotpurch120>0).astype(float),purchtot120,purchtot120/(chaintot+small)))
fpurch180=np.hstack(((purchtotpurch180>0).astype(float),purchtot180,purchtot180/(chaintot+small)))
fpurchtot=np.hstack((fpurch,fpurch10,fpurch30,fpurch60,fpurch90,fpurch120,fpurch180))

brandpurch=np.reshape(brand[0::,0],(size,1))
brandpurch10=np.reshape(brand10[0::,0],(size,1))
brandpurch30=np.reshape(brand30[0::,0],(size,1))
brandpurch60=np.reshape(brand60[0::,0],(size,1))
brandpurch90=np.reshape(brand90[0::,0],(size,1))
brandpurch120=np.reshape(brand120[0::,0],(size,1))
brandpurch180=np.reshape(brand180[0::,0],(size,1))
fbrand=np.hstack(((brandpurch>0).astype(float),brand,brand/(brandtot+small),brand/(purchtot_brand+small)))
fbrand10=np.hstack(((brandpurch10>0).astype(float),brand10,brand10/(brandtot+small),brand10/(purchtot_brand10+small),brand10/(purchtot_brand+small)))
fbrand30=np.hstack(((brandpurch30>0).astype(float),brand30,brand30/(brandtot+small),brand30/(purchtot_brand30+small),brand30/(purchtot_brand+small)))
fbrand60=np.hstack(((brandpurch60>0).astype(float),brand60,brand60/(brandtot+small),brand60/(purchtot_brand60+small),brand60/(purchtot_brand+small)))
fbrand90=np.hstack(((brandpurch90>0).astype(float),brand90,brand90/(brandtot+small),brand90/(purchtot_brand90+small),brand90/(purchtot_brand+small)))
fbrand120=np.hstack(((brandpurch120>0).astype(float),brand120,brand120/(brandtot+small),brand120/(purchtot_brand120+small),brand120/(purchtot_brand+small)))
fbrand180=np.hstack(((brandpurch180>0).astype(float),brand180,brand180/(brandtot+small),brand180/(purchtot_brand180+small),brand180/(purchtot_brand+small)))
fbrandtot=np.hstack((fbrand,fbrand10,fbrand30,fbrand60,fbrand90,fbrand120,fbrand180))

comppurch=np.reshape(comp[0::,0],(size,1))
comppurch10=np.reshape(comp10[0::,0],(size,1))
comppurch30=np.reshape(comp30[0::,0],(size,1))
comppurch60=np.reshape(comp60[0::,0],(size,1))
comppurch90=np.reshape(comp90[0::,0],(size,1))
comppurch120=np.reshape(comp120[0::,0],(size,1))
comppurch180=np.reshape(comp180[0::,0],(size,1))
fcomp=np.hstack(((comppurch>0).astype(float),comp,comp/(comptot+small),comp/(purchtot+small)))
fcomp10=np.hstack(((comppurch10>0).astype(float),comp10,comp10/(comptot+small),comp10/(purchtot10+small),comp10/(purchtot+small)))
fcomp30=np.hstack(((comppurch30>0).astype(float),comp30,comp30/(comptot+small),comp30/(purchtot30+small),comp30/(purchtot+small)))
fcomp60=np.hstack(((comppurch60>0).astype(float),comp60,comp60/(comptot+small),comp60/(purchtot60+small),comp60/(purchtot+small)))
fcomp90=np.hstack(((comppurch90>0).astype(float),comp90,comp90/(comptot+small),comp90/(purchtot90+small),comp90/(purchtot+small)))
fcomp120=np.hstack(((comppurch120>0).astype(float),comp120,comp120/(comptot+small),comp120/(purchtot120+small),comp120/(purchtot+small)))
fcomp180=np.hstack(((comppurch180>0).astype(float),comp180,comp180/(comptot+small),comp180/(purchtot180+small),comp180/(purchtot+small)))
fcomptot=np.hstack((fcomp,fcomp10,fcomp30,fcomp60,fcomp90,fcomp120,fcomp180))

catpurch=np.reshape(cat[0::,0],(size,1))
catpurch10=np.reshape(cat10[0::,0],(size,1))
catpurch30=np.reshape(cat30[0::,0],(size,1))
catpurch60=np.reshape(cat60[0::,0],(size,1))
catpurch90=np.reshape(cat90[0::,0],(size,1))
catpurch120=np.reshape(cat120[0::,0],(size,1))
catpurch180=np.reshape(cat180[0::,0],(size,1))
fcat=np.hstack(((catpurch>0).astype(float),cat,cat/(cattot+small),cat/(purchtot+small)))
fcat10=np.hstack(((catpurch10>0).astype(float),cat10,cat10/(cattot+small),cat10/(purchtot10+small),cat10/(purchtot+small)))
fcat30=np.hstack(((catpurch30>0).astype(float),cat30,cat30/(cattot+small),cat30/(purchtot30+small),cat30/(purchtot+small)))
fcat60=np.hstack(((catpurch60>0).astype(float),cat60,cat60/(cattot+small),cat60/(purchtot60+small),cat60/(purchtot+small)))
fcat90=np.hstack(((catpurch90>0).astype(float),cat90,cat90/(cattot+small),cat90/(purchtot90+small),cat90/(purchtot+small)))
fcat120=np.hstack(((catpurch120>0).astype(float),cat120,cat120/(cattot+small),cat120/(purchtot120+small),cat120/(purchtot+small)))
fcat180=np.hstack(((catpurch180>0).astype(float),cat180,cat180/(cattot+small),cat180/(purchtot180+small),cat180/(purchtot+small)))
fcattot=np.hstack((fcat,fcat10,fcat30,fcat60,fcat90,fcat120,fcat180))

features=np.hstack((fbrandtot,fcomptot,fcattot,fpurchtot,offerfea,weekdate,monthdate,offerdate))

print "Number of features:", np.shape(features)[1]

np.save(string,features)
