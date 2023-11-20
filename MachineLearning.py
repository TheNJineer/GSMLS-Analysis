import sklearn


class MachineLearning:

    def __init__(self):
        # What information do I need to initialize an instance of this class?
        pass

    def svm(self):
        # Uses SKLearn to run Support Vector Matrix model for classification
        # of homes sold based on home price and home type and other home attributes
        pass

    def linearregression(self):
        # Uses SKLearn to run LinearRegression to predict the house price of a home based on home attributes
        pass

    def decisiontree(self):
        """
        Uses SKLearn to run XGBoost Decision Tree to determine approximate rehab price based on rehab needed.
        Create a template for levels of rehab needed (cosmetic, moderate, full gut) to determine material and labor
        prices and overall total for scope of work. Create a df from the Excel worksheet
        comprised of all the material and labor information
        :return:
        """
        pass

    def cnn(self):
        # Uses SKLearn Convolutional Neural Network to use visual ML
        # to determine level of rehab needed based on the pictures provided
        pass

