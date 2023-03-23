import ass1
#q1
df11 = ass1.createdataframe()

df_1 = ass1.unixtime(df11)

df_2 = ass1.todatetype(df_1)

df_3 = ass1.removespace(df_2, df11)

df_4 = ass1.replacenull(df_3)

#q2
df22 = ass1.createdataframe1()

df1 = ass1.camelcasetosnakecase(df22)

df2 = ass1.starttimems(df1)

#q3
final_df = ass1.merge(df2, df_4)

final1_df = ass1.filter(final_df)



