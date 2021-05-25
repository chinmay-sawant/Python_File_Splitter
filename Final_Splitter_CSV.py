
from functools import reduce
import pandas as pd
import configparser
import os
import shutil



config = configparser.ConfigParser()
config.read('Splitter_CSV.ini')

tsplitAt = config['splittercsv']['splitAt']
splitAt=int(tsplitAt)

f=[]
ff=os.listdir("./source/")
print(ff)
for i1 in ff:
    if os.path.isdir(i1):  
        print(i1)
        
        
    else:
        print(i1)
        f.append(i1)
    
print(f)
srcPath="./source/"
desPath="./destination/"
print(f)

fncounter=1
global totalFileCounter,rowsCount
totalFileCounter=0
rowsCount=0
try:
    for i in range(0,len(f)):                                                 #For Loop Here
        print("\nStarting New Executing")
        print(f[i])
        try:
            global r
            r=pd.read_csv(f"{srcPath}{f[i]}").index
            print("Total Rows OF R ",r)
        except pd.errors.EmptyDataError:
            print("Inputed Data File Is Emmpty")
            print("This file will not be copied !!!")
         
            i+=1
            print("\nStarting New Executing")
            print(f[i])
            r=pd.read_csv(f"{srcPath}{f[i]}").index
        
        print(f"hi there is nothing here except {f[i]}")

        print(f"{f[i]} contains {len(r)}")
        
        newFolder=f"{desPath}{f[i]}/"
        newFilename=f"{f[i]}".split(".")
        try:
            newDir=os.mkdir(newFolder)
        except WindowsError as we:
            print(f"Directory Already Exists !!!")
            print(f"Removing Directory {newFolder}")
            shutil.rmtree(newFolder)
            newDir=os.mkdir(newFolder)

        print(f"Before IF filename is {f[i]}")
        if len(r)<=splitAt:   
                                                        #If loop to compare length greater than splitAt
            print(f"Inside IF filename is {f[i]}")
            try:
                readCSV=pd.read_csv(f"{srcPath}{f[i]}")
                print(f"no need to split {f[i]} as the file contains rows less than {splitAt}")
            except pd.errors.EmptyDataError:
                print("Data Frame Empty")     
                print(readCSV)
                print(f"{f[i]} is a empty file !!!")
        else:
            global counter
            rowsCount=0
            counter=0
            while True:
                try: 
                    
                    if counter==0:
                        global rcounterCSV
                        rcounterCSV_t=pd.read_csv(f"{srcPath}{f[i]}",nrows=splitAt).index
                        rcounterCSV=len(rcounterCSV_t)
                        print(f"This Will Repeat Single Time {rcounterCSV}")
                        rCSV=pd.read_csv(f"{srcPath}{f[i]}",nrows=splitAt).to_csv(f"{newFolder}{newFilename[0]}_{fncounter}.csv",index=False)
                    
                        
                        counter+=1
                        rowsCount+=rcounterCSV
                        fncounter+=1
                        totalFileCounter+=1


                    else:
                        rcounterCSV_t=pd.read_csv(f"{srcPath}{f[i]}",skiprows=rowsCount,nrows=splitAt).index
                        rcounterCSV=len(rcounterCSV_t)
                        if rcounterCSV==0:
                            print("No Rows To Read Exiting !!!")
                            break
                        
                        else:
                            print(f"This Will Repeat Every Time {rcounterCSV}")
                            rCSV=pd.read_csv(f"{srcPath}{f[i]}",skiprows=rowsCount,nrows=splitAt,header=0).to_csv(f"{newFolder}{newFilename[0]}_{fncounter}.csv",index=False)
                            counter+=1
                            rowsCount+=rcounterCSV
                            fncounter+=1
                            totalFileCounter+=1
                            i+=1
                            

                except pd.errors.EmptyDataError:
                    print("No Rows to Split !!!")   
                
                
                
                    
                    


    print(f"\nTotal File Count is {totalFileCounter} \nTotal Rows Count {rowsCount}")
except IndexError as ie:
    print("For Loop Ending ...")
    print(f"\nTotal File Count is {totalFileCounter} \nTotal Rows Count {rowsCount}")
    



