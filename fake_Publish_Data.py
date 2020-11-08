from datetime import datetime
from random import randrange


def fake_Publish_Data():
    Data = []

    for i in range(30):
        ShopData = {}
        ShopData['ShopID'] = i + 1
        ShopData['Date_n_Time'] = (datetime.today()).strftime("%Y-%m-%d %H:%M:%S")
        ShopData['CurrentCapacity'] = randrange(0, getMaxCapacity(i + 1))

        Data.append(ShopData)

    return Data


def getMaxCapacity(id):
    if id in [1, 7, 20, 22, 30]:
        return 400
    elif id in [2, 4, 11, 12, 14, 16, 21, 24, 27, 28]:
        return 260
    elif id in [3, 10, 13, 15, 17, 23, 29]:
        return 130
    elif id in [5, 6, 8, 9, 18, 19, 25, 26]:
        return 60
