from textblob import TextBlob

words =["How are you","Hakuna Matata","चिट्ठी भित्र राखेर पठाउँदै छु मेरो माया म तिमीलाई खोली पढि देउन है आटे जती भरेर पानाहरु मायाका कुरा लेखेर मनले जानेको सबै"]
translated =[]

for word in words:
    blob = TextBlob(word)
    try:
        translated.append(blob.translate(to=('en')))
    except:
        translated.append(word)
        


print(translated)