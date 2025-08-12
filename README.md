<div id="header" align="center">
   <img src="https://s3.amazonaws.com/files.usmre.com/gsmls.jpg" width="400"/>
</div>

<b>Welcome to the NJ Real Estate Analysis Project:</b>

This project seeks to be a one-stop shop for real estate agents, home buyers and home 
sellers alike in the state of NJ to generate quick comparable properties as well as gauge a subject 
property's sales price based on its physical and locational characteristics in a dashboard. In addition, one can 
assess local real estate market activity with closed homes (metrics such as homes available, currently 
under contract, withdrawn/expired coming soon) to help recognize what transactions are occurring on a 
weekly basis. This dashboard enlists the help of machine learning algorithms 
to approximate the likelihood of a home being sold and how much it will sell for based on the 
characteristics of the property provided. The regression algorithm has achieved a coefficient of 
determination (r2 Score) of 96.8% and mean absolute percentage error (MAPE) of 5.1% while the 
classification algorithm has achieved a f1 Score of 85%. Efforts are being made daily to increase 
their capabilities and improve results. 

<b>Background</b>

The current system (or lack thereof) for real estate agents to analyze their local and distance markets is inefficient and time consuming. To properly represent clients and meet their needs, an agent needs to spend many hours throughout the week tracking recent sales, inventory, home prices and conducting comparable sales analysis. With this information, an agent can thoroughly understand market dynamics, when, where and how a buyer should purchase a property or to sell a home. However, markets change on a weekly and monthly basis. This can be overwhelming for new and even established agents as New Jersey has 4-5 different private MLS services which organize and display their data with different methods. This inefficiency can lead to increased frustration and lack of understanding with one's job, increased inability to keep up with fast paced markets, decreased client satisfaction and loss of revenue (salary).

The goal of this project is intended to achieve multiple objectives:
- Create a system which automatically collects, cleans and stores real estate data on a weekly basis for each accessible MLS
- Understand the relationship between a property’s sales price and it’s physical and locational characteristics
- Determine if a property will be sold based on sales price and physical and locational characteristics
- Create a dashboard to display market statistics as well as provide the ability to conduct data/market analysis on a state and municipal level
- Obtain a deeper knowledge of the real estate market to not only be more informed, but also be a better asset to buyer and seller clients alike

<b>Project Conclusions:</b>

- The regression DNN captures the 96.8% of the sales price variance from the available data
- The classification DNN has an F1 Score of 85%
- More training data needs to be acquired to improve both models, but especially for the classification. There are still some features needed to help distinguish between a property which is sold/not sold
- Collected data is concentrated in the Northen and Central municipalities. Access to data for South Jersey is necessary for better model generalization and improvement
- Home prices continue to rise YoY while total numbers of home sales continue to decline
- The median days a property spend on the market before being sold hasn’t been above 30 days since 2021. This signifies heavy buyer demand for homes.
- Bank foreclosures have steadily increased since the COVID mortgage moratorium ended in 2021 but has plateaued in 2023 - 2024
- Properties bought by investors in NJ peaked in 2019 and have declined since
- Homes built in the 1950s and 1980s dominate the existing homes market. There has been a lack of new construction homes built since the early 2000s

<b>Next Steps:</b>

- Train a multi-classification CNN to label real estate images for the property’s quality/condition and the area/section of the house 
- Change the “Unknown” value of a property’s “Condition” to the predicted classification label
- Retrain the classification network now using “Condition” as a feature
- Use sentiment analysis on a property’s listing remarks also understand the condition of the house
- Do an analysis of the results of the visual prediction vs the sentiment prediction.
  - Which one more accurately depicts the quality of the house? Can I use the sentiment prediction as a regularization (of some sort) for the image quality prediction?
- Create dashboard to monitor the results of the neural networks and keep track of each iteration and change log of the retraining/updates
