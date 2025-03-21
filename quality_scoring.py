import pandas as pd


df = pd.read_csv("titanic.csv")
print(df.head())

# วิธีการเรียกดูค่าใน Column
# df["Survived"]
# df.Survived

df.info()

PassengerId_not_null = df.PassengerId.notnull()
dq_PassengerId = PassengerId_not_null.sum() / len(df)
print(f"Data Quality of PassengerId: {dq_PassengerId}")

Survived_not_null = df.Survived.notnull()
dq_Survived = Survived_not_null.sum() / len(df)
print(f"Data Quality of Survived: {dq_Survived}")

Pclass_not_null = df.Pclass.notnull()
dq_Pclass = Pclass_not_null.sum() / len(df)
print(f"Data Quality of Pclass: {dq_Pclass}")

Name_not_null = df.Name.notnull()
dq_Name = Name_not_null.sum() / len(df)
print(f"Data Quality of Name: {dq_Name}")

Sex_not_null = df.Sex.notnull()
dq_Sex = Sex_not_null.sum() / len(df)
print(f"Data Quality of Sex: {dq_Sex}")

age_not_null = df.Age.notnull()
dq_age = age_not_null.sum() / len(df)
print(f"Data Quality of Age: {dq_age}")

SibSp_not_null = df.SibSp.notnull()
dq_SibSp = SibSp_not_null.sum() / len(df)
print(f"Data Quality of SibSp: {dq_SibSp}")

Parch_not_null = df.Parch.notnull()
dq_Parch = Parch_not_null.sum() / len(df)
print(f"Data Quality of Parch: {dq_Parch}")

Ticket_not_null = df.Ticket.notnull()
dq_Ticket = Ticket_not_null.sum() / len(df)
print(f"Data Quality of Ticket: {dq_Ticket}")

Fare_not_null = df.Fare.notnull()
dq_Fare = Fare_not_null.sum() / len(df)
print(f"Data Quality of Fare: {dq_Fare}")

cabin_not_null = df.Cabin.notnull()
dq_cabin = cabin_not_null.sum() / len(df)
print(f"Data Quality of Cabin: {dq_cabin}")

embarked_not_null = df.Embarked.notnull()
dq_embarked = embarked_not_null.sum() / len(df)
print(f"Data Quality of Embarked: {dq_embarked}")

print(f"Completeness: {(dq_PassengerId + dq_Survived + dq_Pclass + dq_Name + dq_Sex + dq_age + dq_SibSp + dq_Parch + dq_Ticket + dq_Fare + dq_cabin + dq_embarked) / 12}")
