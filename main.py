import json
import os
import time
import threading
l=threading.Lock() #to make thread safe
def create(key,value,ttl=0,new=0):
    with l:
        count=0
        size=os.stat("values.txt").st_size
        if (size < 1024 * 1024 * 1024): #to not exceed 1gb
            if (len(key) < 32):
                if (value <= 16 * 1024 * 1024):
                    if(new==0):  #to append the existing datastore
                        f=open("values.txt","r")
                        data = f.read().split('\n')
                        f.close()
                        for line in data:
                            line = line.split(' ')
                            if (line[0] == key):
                                print(key + " is already present")
                                break
                            else:
                                count +=1 
                    elif(new == 1):  #to create a new datastore
                        f = open("values.txt", "w").close()
                        data=[]
                    temp=[]
                    if (count == len(data)):
                        if (ttl == 0):
                            temp = [str(value), str(ttl)]
                        elif (ttl > 0):
                            temp = [str(value), str(time.time() + ttl)]
                    if(new == 1):
                        fr = open("values.txt", "w")
                    elif(new == 0):
                        fr = open("values.txt", "a")
                    fr.write(key + " " + ' '.join(temp))
                    fr.write('\n')
                    fr.close()
                else:
                    print("value exceeded the memory limit")
            else:
                print("key is longer than expected")
        else:
            print("file size exceeded the memory limit")

def destroy(key):
    with l:
        count=1
        fs = open("values.txt", "r")
        temp1 = fs.read().split('\n')
        fs.close()
        for i in range(len(temp1)):
            data = (temp1[i].split(' '))
            if (len(data) > 1):
                if(data[0]==key):
                    if(data[2] != '0'):
                        if (time.time() >= float(data[2])): #checking the time limit 
                            print("Time limit of " + key + " is expired!!")
                    else:
                        fw = open("values.txt", "w").close()
                        fr = open("values.txt", "a")
                        for i in range(len(temp1)):
                            data1 = (temp1[i].split(' '))
                            if (len(data1) > 1):
                                if (data1[0] != key):   #to delete the key value pair
                                    fr.write(' '.join(data1))
                                    fr.write('\n')
                                else:
                                    continue
                        fr.close()
                        print(key +" is Deleted successfully!")
                else:
                    count+=1
        if(count==len(temp1)):
            print(key+" does not exist in the datastore !!")

def read(key):
    with l:
        count=1
        fy = open("values.txt", "r")
        temp1 = fy.read().split('\n')
        fy.close()
        for i in range(len(temp1)):
            temp = temp1[i]
            data = (temp1[i].split(' '))
            if (len(data) > 1):
                if(data[0]==key):
                    if (data[2] != '0'):
                        if (time.time() >= float(data[2])):
                            print("Time limit of " + key + " is expired!!")   
                            break
                    d={'key':key,'value':data[1]}  #printing the value in json format
                    print(d)
                else:
                    count=count+1
        if(count==len(temp1)):
            print(key + " is not present in the datastore!")
