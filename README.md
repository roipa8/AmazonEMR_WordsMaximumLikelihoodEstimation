# AmazonEMR_WordsMaximumLikelihoodEstimation
# How To Run
1. unzip the submission file
2. run java -jar Assignment2.jar

### Program Flow

* Step1:

      Input: a sequential file (hebrew, 3gram).     
      Args: localAggregationMode, input and output.
      Job: Parsing the input in to (3gram, data_table), where the data_table will include after the reduce: (3gram, occurrences) and (w1w2, occurrences).
      Stats:
        With Local Aggregation: 
          Map output records = 142239026
          Map output bytes = 5974781556
          Combine input records = 145155529
          Combine output records = 5816529
          Reduce input records = 2900026
          Reduce output records = 1686118

        Without Local Aggregation: 
          Map output records=142239026
          Map output bytes=5974781556 
          Reduce input records=142239026
          Reduce output records=1686118

* Step2:

        Input: a sequential file (3gram, data_table)*.
        Job: counting w2 and w2w3 occurrences, the output will be (3gram, data_table), where the data_table will include after the reduce: (3gram, occurrences),   (w1w2,occurrences), (w2, occurrences) and (w2w3, occurrences). 
        Stats:
          With Local Aggregation:
            Map output records=5058354
            Map output bytes=220542712
            Combine input records=5058354
            Combine output records=2354489
            Reduce input records=2354489
            Reduce output records=1686118
            
          Without Local Aggregation: 
            Map output records=5058354
            Map output bytes=220542712
            Reduce input records=5058354
            Reduce output records=1686118
        
 
 * Step3:
 
       Input: a sequential file (3gram, data_table)*.
       Job: counting w3 occurrences and number of words in the corpus, the output will be (3gram, data_table), where the data_table will include after the reduce: (3gram,occurrences), (w1w2, occurrences), (w2, occurrences), (w2w3, occurrences), (w3, occurrences) and (wordsInCorpus, occurrences).
       Stats:
          With Local Aggregation:
            Map output records=5058354
            Map output bytes=233131608
            Combine input records=5058354
            Combine output records=1777553
            Reduce input records=1777553
            Reduce output records=1686118
            
          Without Local Aggregation: 
            Map output records=5058354
            Map output bytes=233131608
            Reduce input records=5058354
            Reduce output records=1686118


 * Step4:

       Input: a sequential file (3gram, data_table)*.
       Job: calculating the probabilty based on the data_table parameters for each 3gram. and sorting it with our compareTo. The result is in text format        
  
# Implementaion Method
 We used the "pairs method", as learned in class: the general idea is to use "*" to sort our pairs so the reduce method will recive first the pairs we will want to sum up.
 With this method we only used O(1) memory.
 
 # Result

    Pair 1 
    הוא נחשב לאחד	0.1620862570969165
    הוא נחשב בעיני	0.09098672196723147
    הוא נחשב בין	0.057483911837370026
    הוא נחשב כאילו	0.04915907764325472
    הוא נחשב על	0.047664774959201885

    Pair 2
    אדם יש לו	0.5996543441662386
    אדם יש בו	0.07836000082740184
    אדם יש להם	0.055003982402376424
    אדם יש חכמה	0.032629239205674475
    אדם יש זכות	0.02494431945764183
    
    Pair 3
    בכל לילה ולילה	0.4453406405995747
    בכל לילה על	0.06334222022018565
    בכל לילה קודם	0.0528120516391317
    בכל לילה עד	0.0494640647966153
    בכל לילה היה	0.046417060559418896

    Pair 4
    גם אני לא	0.129015865371063
    גם אני הייתי	0.0830519911128784
    גם אני רוצה	0.04230193728923052
    גם אני את	0.029381552539205193
    גם אני יודע	0.015432889449451113

    Pair 6
    דרך שער יפו	0.1412014020125241
    דרך שער ציון	0.10658946639428858
    דרך שער האריות	0.09211010199014139
    דרך שער זה	0.08469318752787917
    דרך שער שכם	0.058781465911033594

    Pair 7
    המיעוט היהודי בארץ	0.2924492556335937
    המיעוט היהודי בפולין	0.16894441521496614
    המיעוט היהודי בארצות	0.1616690297946443
    המיעוט היהודי על	0.14103189446392891
    המיעוט היהודי שלטון	0.10633603699399274

    Pair 8
    רחמים על ישראל	0.16725841998728636
    רחמים על עצמו	0.16375907548582352
    רחמים על חבירו	0.05561559925707607
    רחמים על כל	0.05308333642871588
    רחמים על בני	0.03393786464305798

    Pair 9
    עונה על השאלה	0.1888085970511244
    עונה על כל	0.10659500091030635
    עונה על כך	0.07411805243318391
    עונה על הצרכים	0.061354342510194164
    עונה על השאלות	0.058625167682281806

    Pair 10
    כואב לי הראש	0.2504180569445102
    כואב לי הלב	0.196664423650532
    כואב לי מאוד	0.1825054206674627
    כואב לי כל	0.09379476287891625
    כואב לי על	0.07787319817029242

