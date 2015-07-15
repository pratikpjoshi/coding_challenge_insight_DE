
# This program focuses on storage efficiency for processing within evry tweet 
#and time efficiency while processing successive tweets. It minimizes the use of string storage and comparisons. 
#It operates tweet by tweet to calulate number of distinct words, their median and their frequency.
#A unique hash key is calculated for every word within each tweet followed by calculation of two word weights by 
#summing all characters in the unique hash key and using first 4 characters of unique hash key for making every word
#disticnctly countable and seperable with min comparisons and storage. The program is scalable for file of any 
#number of tweets and focusses on use of pandas dataframes in python which is a scalable datastructure preferred in big
#data type of operations. The example input tweets and corresponsing output tweets have been obtained in github repositories.
#its a unique approach which provides a good technical tradeoff and balance of time and storage efficiency.

import numpy as np
import pandas as pd
import fileinput
import hashlib
from math import *

#Define max word weight dataframe with 150 characters size limit of twitter.
max_weight = (127)*150
max_weight2 = (127)*4
df = pd.DataFrame([[[]]*max_weight,[[]]*max_weight,[[]]*max_weight,[[]]*max_weight],index=('freq','lin_num','wrd_num','wrd_len'))
med_f=pd.DataFrame([[[]],[[]]],index=('dw','m'))
df_median=pd.DataFrame([[[]],[[]]],index=('med','f'))
all_tweet_df=pd.DataFrame([[[]],[[]]],index=('w','f'))
line_number = 0
ind_number = 0
aa=[]

#Take input and read it word by word in every line
for line in fileinput.input(['/Users/Pratik/Desktop/tweets.txt']):
    lines = [np.array(map(str, line.split()))]
    line_number = line_number + 1
    ww1=pd.DataFrame([[[]]*max_weight,[[]]*max_weight,[[]]*max_weight,[[]]*max_weight],index=('freq','lin_num','wrd_num','wrd_len'))
    ww2=np.zeros((max_weight,max_weight2))
    line_word_map=pd.DataFrame([[[]],[[]],[[]]],index=('wrd','ww1','ww2'))
    

    for wrd in range(0,len(lines[0])-1):
        hash_object1 = hashlib.sha1((str(lines[ind_number][wrd])).lower())
        hash_object = hashlib.new('DSA')
        hash_object.update(str(hash_object1.hexdigest()))
        encrypt_word = hash_object.hexdigest()
        word_weight=sum([ord(c) for c in encrypt_word])
        word_weight2=sum([ord(c) for c in encrypt_word[0:4]])
        
        if (df.loc['freq',word_weight]==[]):
            df.loc['freq',word_weight]=[1]
            df.loc['lin_num',word_weight]=[line_number]
            df.loc['wrd_num',word_weight]=[wrd]
        else:
            df.loc['freq',word_weight].append(1)
            df.loc['lin_num',word_weight].append(line_number)
            df.loc['wrd_num',word_weight].append(wrd)
            
        if (ww1.loc['freq',word_weight]==[]):
            ww1.loc['freq',word_weight]=[1]
            ww1.loc['wrd_num',word_weight]=[wrd]
            ww2[word_weight,word_weight2]=1
            
        else:
            ww1.loc['freq',word_weight].append(1)
            ww1.loc['wrd_num',word_weight].append(wrd)
            ww2[word_weight,word_weight2]=ww2[word_weight,word_weight2]+1

        if (line_word_map.loc['wrd',0]==[]):
            line_word_map.loc['wrd',0]=[lines[ind_number][wrd]]
            line_word_map.loc['ww1',0]=[word_weight]
            line_word_map.loc['ww2',0]=[word_weight2]
        
        else:
            line_word_map.loc['wrd',0].append(lines[ind_number][wrd])
            line_word_map.loc['ww1',0].append(word_weight)
            line_word_map.loc['ww2',0].append(word_weight2)
            
        aa.append([lines[ind_number][wrd],word_weight,word_weight2,encrypt_word]) 
        
    for word_wght in range(0,max_weight):
        df.loc['wrd_len',word_wght]= len(df.loc['wrd_num',word_wght])
    local_multiple_word_indices=np.where(df.loc['wrd_len':]>1)
    for word_wght in range(0,max_weight):
        ww1.loc['wrd_len',word_wght]= len(ww1.loc['wrd_num',word_wght])
    ww1_multiple_word_indices=np.where(ww1.loc['wrd_len':]>1)

    k=0
    ww2_word_ind=[[]]*max_weight
    for word_wght in range(2500,3000):
      for word_wght2 in range(0,max_weight2):
        if(ww2[word_wght,word_wght2]>1.0):
            if(ww2_word_ind[word_wght]==[]):
                ww2_word_ind[word_wght]=[word_wght2]
            else:
                ww2_word_ind[word_wght].append(word_wght2)
                
    freq_f=pd.DataFrame([[[]],[[]],[[]],[[]]],index=('word','freq','weight1','weight2'))
    freq_f2=pd.DataFrame([[[]],[[]],[[]],[[]]],index=('word','freq','weight1','weight2'))
    temp_f=pd.DataFrame([[[]],[[]],[[]],[[]]],index=('word','freq','weight1','weight2'))
    new_f=pd.DataFrame([[[]],[[]],[[]],[[]]],index=('w','f','w1','w2'))
    freq_f2=freq_f
    temp_f=freq_f


    for word_wght_ind in range(0,len(ww1_multiple_word_indices[1])):
      if (len(ww2_word_ind[ww1_multiple_word_indices[1][word_wght_ind]])==0):
        ind=np.where(ww2[ww1_multiple_word_indices[1][word_wght_ind],:]==1.0)

        for i in range(0,len(ind[0])):
            if(freq_f.loc['weight1',0]==[]):
                freq_f.loc['freq',0]=[1.0]
                freq_f.loc['weight1',0]=[ww1_multiple_word_indices[1][word_wght_ind]]
                freq_f.loc['weight2',0]=[ind[0][0]]
            else:
                freq_f.loc['weight1',0].append(ww1_multiple_word_indices[1][word_wght_ind])
                freq_f.loc['weight2',0].append(ind[0][i])
                freq_f.loc['freq',0].append(1.0)
                med_f.loc['dw',0]=[len(ind[0])]
                
        for j in range(0,len(ind[0])):
            for i in range(0,len(line_word_map.loc['ww1',0])):
                if (line_word_map.loc['ww1',0][i]==ww1_multiple_word_indices[1][word_wght_ind] and line_word_map.loc['ww2',0][i]==ind[0][j]):
                    if freq_f.loc['word',0]==[]:    
                        freq_f.loc['word',0]=[line_word_map.loc['wrd',0][i]]
                    else:
                        freq_f.loc['word',0].append(line_word_map.loc['wrd',0][i])
      if (len(ww2_word_ind[ww1_multiple_word_indices[1][word_wght_ind]])>0):
        ind2=np.where(ww2[ww1_multiple_word_indices[1][word_wght_ind],:]>1.0)
        
        for i in range(0,len(ind2[0])):
            if(freq_f.loc['weight1',0]==[]):
                freq_f.loc['weight1',0]=[ww1_multiple_word_indices[1][word_wght_ind]]
                freq_f.loc['weight2',0]=[ind2[0][i]]
                freq_f.loc['freq',0]=[ww2[ww1_multiple_word_indices[1][word_wght_ind],ind2[0][i]]]
            else:
                freq_f.loc['weight1',0].append(ww1_multiple_word_indices[1][word_wght_ind])
                freq_f.loc['weight2',0].append(ind2[0][i])
                freq_f.loc['freq',0].append(ww2[ww1_multiple_word_indices[1][word_wght_ind],ind2[0][i]])
        
        
        for j in range(0,len(ind2[0])):
            word_count=0
            for i in range(0,len(line_word_map.loc['ww1',0])):
                if (line_word_map.loc['ww1',0][i]==ww1_multiple_word_indices[1][word_wght_ind] and line_word_map.loc['ww2',0][i]==ind2[0][j]):
                    word_count=word_count+1
                    if (word_count==1):
                        freq_f.loc['word',0].append(line_word_map.loc['wrd',0][i])
        
    false_repeat_count=0.0
    for i in range(0,len(freq_f.loc['freq',0])):
        if(freq_f.loc['freq',0][i]==1.0):
            false_repeat_count +=1

#calculate distinct words in every line (i.e. tweet)
    dist_words=len(lines[0])-len(freq_f.loc['word',0])+false_repeat_count
    if med_f.loc['dw',0]==[]:
        med_f.loc['dw',0]=[dist_words]
    else:
        med_f.loc['dw',0].append(dist_words)
        
#Calculate running median
    running_len=len(med_f.loc['dw',0])
    if running_len<=1:
        if med_f.loc['m',0]==[]:
            med_f.loc['m',0]=[med_f.loc['dw',0][0]]
        else:
            med_f.loc['m',0].append(med_f.loc['dw',0][0])
            
    if running_len>1 and running_len<=2:
        med_val=0.5*(med_f.loc['dw',0][0]+med_f.loc['dw',0][1])
        if med_f.loc['m',0]==[]:
            med_f.loc['m',0]=[med_val]
        else:
            med_f.loc['m',0].append(med_val)
                
    if running_len>2:
        bbb=sorted(med_f.loc['dw',0])
        len_var=len(med_f.loc['dw',0])
        if (len_var%2)==0:
            med_var=0.5*(bbb[len_var/2]+bbb[(len_var-2)/2])
        if (len_var%2)==1:
            med_var=bbb[(len_var-1)/2]
        if med_f.loc['m',0]==[]:
            med_f.loc['m',0]=[median_var]
        else:
            med_f.loc['m',0].append(med_var)

    new_f.loc['w',0]=freq_f.loc['word',0]
    new_f.loc['f',0]=freq_f.loc['freq',0]
    new_f.loc['w1',0]=freq_f.loc['weight1',0]
    new_f.loc['w2',0]=freq_f.loc['weight2',0]

    cnt2=0
    for wrd in range(0,len(lines[0])):
        if (len(freq_f.loc['word',0])==0):
            if(new_f.loc['w',0]==[]):
                new_f.loc['w',0]=[lines[0][wrd]]
                new_f.loc['f',0]=[1.0]
            else:
                new_f.loc['w',0].append(lines[0][wrd])
                new_f.loc['f',0].append(1.0)
        if (len(freq_f.loc['word',0])>0):
            cnt1=0
            cnt3=0
            for rep in range(0,len(freq_f.loc['word',0])):
                if (lines[0][wrd]==freq_f.loc['word',0][rep]):
                    cnt1=cnt1+1
            if (cnt1==0):
                new_f.loc['w',0].append(lines[0][wrd])
                new_f.loc['f',0].append(1.0)
                cnt3+=1

    for i in range(0,len(new_f.loc['w',0])):
        if (all_tweet_df.loc['w',0]==[]):
            all_tweet_df.loc['w',0]=[new_f.loc['w',0][i]]
            all_tweet_df.loc['f',0]=[new_f.loc['f',0][i]]
        else:
            all_tweet_df.loc['w',0].append(new_f.loc['w',0][i])
            all_tweet_df.loc['f',0].append(new_f.loc['f',0][i])
    

# define a simple function for getting indices of repeted words in a dataframe list

def duplicate_indices_from(inp_list,word):
    initiate = -1
    indices_loc = []
    while True:
        try:
            srch_loc = inp_list.index(word,initiate+1)
        except ValueError:
            break
        else:
            indices_loc.append(srch_loc)
            initiate = srch_loc
    return indices_loc
#......................................

#Sort and Count the word frequency on after combining all the tweets 

yyy=sorted(all_tweet_df.loc['w',0])
new_t_df=pd.DataFrame([[[]],[[]],[[]]],index=('wr','fr','co'))

for i in range(0,len(all_tweet_df.loc['w',0])):
    if (new_t_df.loc['wr',0]==[]):
        new_t_df.loc['wr',0]=[all_tweet_df.loc['w',0][i]]
        new_t_df.loc['fr',0]=[all_tweet_df.loc['f',0][i]]
        new_t_df.loc['co',0]=[0]
    else:
        new_t_df.loc['wr',0].append(all_tweet_df.loc['w',0][i])
        new_t_df.loc['fr',0].append(all_tweet_df.loc['f',0][i])
        new_t_df.loc['co',0].append(1)
        
a=[]
i=0
while True:
    wrd_rep_ind = duplicate_indices_from(new_t_df.loc['wr',0],new_t_df.loc['wr',0][i])
    cntrr=0
    while(len(wrd_rep_ind)>1):
                indi=0
                cntrr=cntrr+1
                if cntrr==1:
                    new_t_df.loc['fr',0][i]=new_t_df.loc['fr',0][wrd_rep_ind[indi]]+new_t_df.loc['fr',0][wrd_rep_ind[indi+1]]
                if cntrr>1:
                    new_t_df.loc['fr',0][i]+=new_t_df.loc['fr',0][wrd_rep_ind[indi+1]]
                new_t_df.loc['wr',0].pop(wrd_rep_ind[indi+1])
                new_t_df.loc['fr',0].pop(wrd_rep_ind[indi+1])
                new_t_df.loc['co',0].pop(wrd_rep_ind[indi+1])
                wrd_rep_ind = duplicate_indices_from(new_t_df.loc['wr',0],new_t_df.loc['wr',0][i])              
    i=i+1
    temp_var=len(new_t_df.loc['wr',0])
    if i==temp_var:
        break

for i in range(0,len(new_t_df.loc['wr',0])):
    count_ind = duplicate_indices_from(new_t_df.loc['wr',0],new_t_df.loc['wr',0][i])
    new_t_df.loc['co',0][i]=count_ind[0]

final_oup_df=pd.DataFrame([[[]],[[]],[[]]],index=('wr','fr','ind'))

final_oup_df.loc['wr',0]=sorted(new_t_df.loc['wr',0])

for i in range(0,len(final_oup_df.loc['wr',0])):
    match_ind = duplicate_indices_from(new_t_df.loc['wr',0],final_oup_df.loc['wr',0][i])
    if final_oup_df.loc['wr',0]==[]:
        final_oup_df.loc['fr',0]=new_t_df.loc['fr',0][match_ind[0]]
        final_oup_df.loc['ind',0]=new_t_df.loc['co',0][match_ind[0]]
    else:
        final_oup_df.loc['fr',0].append(new_t_df.loc['fr',0][match_ind[0]])
        final_oup_df.loc['ind',0].append(new_t_df.loc['co',0][match_ind[0]])
 
#print the resulting dataframe of words with their respective frequencies and indices within the unsorted dataframe

print "ALL TWEET_WORD_AND_COUNT_DATAFRAME", 
print final_oup_df
print "MEDIAN_DATAFRAME"
print med_f

#Write to respective output text files

file = open('/Users/Pratik/Desktop/ft1.txt', "w")
for i in range(0,len(final_oup_df.loc['wr',0])):
    file.write(final_oup_df.loc['wr',0][i]+"\t")
    file.write(str(final_oup_df.loc['fr',0][i])+"\n")
file.close()

file = open('/Users/Pratik/Desktop/ft2.txt', "w")
file.write("Distinct words"+"\t\t"+"Running Median")
file.write("\n\n")
for i in range(0,len(med_f.loc['dw',0])):
    file.write(str(med_f.loc['dw',0][i])+"\t\t\t\t"+str(med_f.loc['m',0][i])+"\n")
    #file.write(str(final_oup_df.loc['fr',0][i])+"\n")
file.close()

