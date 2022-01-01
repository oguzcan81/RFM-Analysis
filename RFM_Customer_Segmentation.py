import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.5f' % x)

df_= pd.read_excel("Datasets/online_retail_II.xlsx", sheet_name= "Year 2010-2011")

### 1 - UNDERSTANDING AND PREPARING DATA

##  1.a - Storing the raw data in a different dataframe is always reasonable in case of losing any data.
df=df_.copy()


# 1.b Examine descriptive statistics of data

df.head()

df.shape

df.describe().T

df.columns

# Invoice : There is a unique number for each invoice. If the invoice number starts with the letter C it stands for canceled transactions
# StockCode: There is a unique number for each product.
# Description: Product name
# Quantity: Indicates how many products were sold.
# InvoiceDate: Invoice date
# Price: Product unit price (Pound)
# Customer ID: Unique customer number
# Country: Name of country where customer lives



# 1.c Are there any missing observations in the dataset? If so, how many missing observations are there in each variable?

df.isnull().sum() ## 135080 records for "Customer ID"  and  1454 records for "Description"

# 1.d  Exclude missing observetions from the dataset. Use "inplace =True" for making our changes permanent.
 #  When you examine dataset deeply, you will notice that there are some bad data for our RFM analiysis.
 #  Firstly , we have to be aware of  negative values in Quantity and Price columns.
 #  Second task is to exclude canceled transactions from the dataframe, bacause those records are not useful for our RFM analysis.


df["Invoice"].head()

df.dropna(inplace=True)

df = df[(df['Quantity'] > 0)]

df = df[(df['Price'] > 0)]

df = df[~df["Invoice"].astype(str).str.contains("C", na=False)]



df.isnull().sum()    ##  Check out dataset after cleaning process .

# 1.e Have a look at number of unique products

df["StockCode"].nunique()


# 1.f Let's see how many of each product there are

df["StockCode"].value_counts()


df.groupby("StockCode").agg("count").sort_values("Quantity", ascending=False).head ()  ## 2nd way



# 1.g Let's list the 5 most ordered products in descending order

df.pivot_table (values="Quantity" , index="StockCode", aggfunc="count").sort_values("Quantity", ascending=False).head(5)  # Also 3rd way for 1.f :)

# 1.h Create a variable named 'Total Price' that represents the total earnings per invoice in order to use in upcoming processes of RFM Analysis

df["TotalPrice"] = df["Quantity"] * df["Price"]


###  2 - CALCULATING RFM METRICS

# Each metric will be calculated specifically for each customer.
# Let's define Recency, Frequency and Monetary metrics that milestones for RFM Analysis process


 #  Recency => How recent was the customer's last purchase?  Number of days between analysis date and last invoice date

 #  Frequency=> How often did this customer make a purchase in a given period? Number of purchases.

 #  Monetary  =>  How much money did the customer spend in a given period? In other words , How much money will the company potentially earn from a particular customer?

 #For recency,  we should know the newest date of invoice in our dataset. According to that date, we can specify analysis date which will be using for next steps of our analysis.
    #Maximum date is 2011-12-09 , Instead of using today date, we decide the most rational date , so I choose 2 days later of maximum date.


df["InvoiceDate"].max()


today_date = dt.datetime(2011, 12, 11)

rfm = df.groupby('Customer ID').agg({'InvoiceDate': lambda InvoiceDate: (today_date - InvoiceDate.max()).days,
                                     'Invoice': lambda Invoice: Invoice.nunique(),
                                     'TotalPrice': lambda TotalPrice: TotalPrice.sum()})




##  Control if there is any bad data.
rfm=rfm[rfm["TotalPrice"] > 0]

##  We made our column names suitable for rfm analysis
rfm.columns = ['recency', 'frequency', 'monetary']

rfm.head()

###  3 - CALCULATING RFM SCORES

# In this part of my article , we will be calculating scores and standardising RFM metrics we formed last part.

# qcut is a Quantile-based discretization function. It divides data into equal sized bins.

## Let's add new columns to dataframe to keep RFM scores.

##   if a customer has made a recent purchase, it is more valueable and should have a higher score. So there is an inverse proportional relationship between frequency score and label.
rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])


##  We use rank method here , why ? I answer your question with one example . Assume that we have a variable consist of a hundred piece "10" value.
##  Even if you rank/quantile that dataset, you will possibly see "10" values in more than one different quentiles.
#  That's why we use the rank method because we want the distribution to be correct.
rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])


rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

# In this business problem , I omit monetary score from the calculation.
# We'll be focussing on recency and frequency scores.

# As you might think, the highest customer score may be 55, and the lowest is 11.
rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) +  rfm['frequency_score'].astype(str))

rfm[rfm["RFM_SCORE"] == "55"].head()



###  4 - CREATING & ANALYSING RFM SEGMENTS

#  We formed/mapped our RFM Segmentation heatmap/graph in code blocks via using Regex

# Naming / Mapping RFM Segmentations
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}


rfm['segment'] = rfm['RFM_SCORE'].replace(seg_map, regex=True)



##  The rest is up to the request comes from manager/business teams.
#   If you want you can set-apart  champions segment then you can send them congratulatory address or evaluate your hibernating customers separately.Based on requirements is changeable.

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

rfm[rfm["segment"] == "need_attention"].head()

rfm.head()

##  We can also extract "new customers" customer id's from the dataframe in order to load into a new csv file named new_customers.csv
rfm[rfm["segment"] == "new_customers"].index


new_df = pd.DataFrame()
new_df["new_customer_id"] = rfm[rfm["segment"] == "new_customers"].index

new_df.to_csv("new_customers.csv")
