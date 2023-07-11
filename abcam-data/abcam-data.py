import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import array, col, coalesce, monotonically_increasing_id, explode

#Initiate session
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

#Create a dynamic frame from the table(and db) created by the AWS crawler service
dyftable_node1 = glueContext.create_dynamic_frame.from_catalog(database='abcam-db', table_name='uniprot_sprot_xml')
dyftable_node1.printSchema()

#Select key columns from the dynamic framce
columns_to_select = ['accession', 'gene.name', 'protein.recommendedName', 'protein.alternativeName', 'sequence' ]
selected_dyf = SelectFields.apply(frame = dyftable_node1, paths = columns_to_select, transformation_ctx = "selected_dyf" )

#check out the structure of the dynamic frame
selected_dyf.printSchema()

#convert the dynamic frame to spark dataframe
selected_df= selected_dyf.toDF()

selected_df.printSchema()

#Select the subset columns/arrays/structs needed
df2 = selected_df.select("accession.*","gene.name.*","protein.recommendedName.fullName.*", "protein.recommendedName.shortName.*",
                         "protein.alternativeName.*",
                         col("sequence._VALUE").alias("sequence"), col("sequence._mass").alias("protein_mass"))
df2.printSchema()


#Rename the columns
df3= df2.toDF("sec_accessn_arr","pri_accessn","gene_name_arr","gene_name_str","protein_recomName_fullnm",
                      "protein_recomName_fullnm_arr", "protein_recomName_shortnm_arr","protein_recomName_shortnm", 
                      "protein_recomName_shortnm_stru", "protein_altName_arr","protein_altName_stru","sequence", "protein_mass")

df3.printSchema()

#Transform the data, unpack structs and arrays
df4= df3.select("sec_accessn_arr","pri_accessn","gene_name_arr",col("gene_name_str._VALUE").alias("primary_gene_name"),
                       "protein_recomName_fullnm",col("protein_recomName_fullnm_arr._VALUE").alias("other_protein_recomName_fullnm"),
                       "protein_recomName_shortnm",
                       # col("protein_recomName_shortnm_arr._VALUE").alias("other_protein_rec_shortname1"),
                       col("protein_recomName_shortnm_stru._VALUE").alias("other_protein_rec_shortname"),
                       col("protein_altName_stru.fullName").alias("protein_altName_fullName"),
                       col("protein_altName_stru.shortName").alias("protein_altName_shortName"),
                       "sequence", "protein_mass")\
            .withColumn("sec_accession_0", df3.sec_accessn_arr[0])\
            .withColumn("sec_accession_1", df3.sec_accessn_arr[1])\
            .withColumn("sec_accession_2", df3.sec_accessn_arr[2])\
            .withColumn("sec_accession_3", df3.sec_accessn_arr[3])\
            .withColumn("gene_name_1", df3.gene_name_arr[0])\
            .withColumn("gene_name_2", df3.gene_name_arr[1])\
            .withColumn("gene_name_3", df3.gene_name_arr[2])\
            .withColumn("primary_accession",coalesce("pri_accessn","sec_accession_0"))
            

df4.printSchema()
df4.count()

#Select needed columns from unpacked data and unpack deeper arrays/structs datatypes
df5= df4.select("primary_accession",
                    # col("pri_accessn").alias("primary_accession"),col("sec_accession_0").alias("sec_accession_0"),
                    "sec_accession_1","sec_accession_2","sec_accession_3",
                    "primary_gene_name.*",
                    # "primary_gene_name",col("gene_name_1._VALUE").alias("sec_gene_name_0"),
                    "gene_name_2._VALUE.*", "gene_name_3._VALUE.*",
                    col("protein_recomName_fullnm").alias("primary_protein_fullname"),col("other_protein_recomName_fullnm").alias("other_protein_rec_fullname"),
                    col("protein_recomName_shortnm").alias("protein_rec_shortname"), "other_protein_rec_shortname",
                    "protein_altName_fullName.*","protein_altName_shortName.*",
                   "sequence", "protein_mass"
                   )


df5.printSchema()
                   
df5.count()

#Rename the columns from df5 accordingly
df6 = df5.toDF("primary_accession","sec_accession_1","sec_accession_2","sec_accession_3",
                    "primary_gene_name_db","primary_gene_name_int", "primary_gene_name_str",
                    "sec_gene_name_1_int", "sec_gene_name_1_str", "sec_gene_name_2_int", "sec_gene_name_2_str",
                    "primary_protein_fullname","other_protein_rec_fullname",
                    "protein_rec_shortname", "other_protein_rec_shortname",
                    "protein_altName_fullName_db","protein_altName_fullName_int", "protein_altName_fullName_str",
                    "protein_altName_fullName_struc",
                    "protein_altName_shortName_arr", "protein_altName_shortName_str",
                    "protein_altName_shortName_struc",
                    "sequence", "protein_mass")

df6.printSchema()

#Add an id column, coalesce alternating columns and select the final df
df7 = df6.select("*").withColumn("id", monotonically_increasing_id())\
         .select("id","primary_accession","sec_accession_1","sec_accession_2","sec_accession_3",
                 # "primary_gene_name_db","primary_gene_name_int", 
                 col("primary_gene_name_str").alias("primary_gene_name"),
                 # "sec_gene_name_1_int", 
                 col("sec_gene_name_1_str").alias("sec_gene_name_1"), 
                 # "sec_gene_name_2_int", 
                 col("sec_gene_name_2_str").alias("sec_gene_name_2"),
                 "primary_protein_fullname","other_protein_rec_fullname",
                 "protein_rec_shortname", "other_protein_rec_shortname",
                 # "protein_altName_fullName_db","protein_altName_fullName_int", 
                 col("protein_altName_fullName_str").alias("protein_altName_fullName_0"),
                 col("protein_altName_fullName_struc._VALUE").alias("protein_altName_fullName_1"),
                 col("protein_altName_shortName_str").alias("protein_altName_shortName_0"),
                 col("protein_altName_shortName_struc._VALUE").alias("protein_altName_shortName_1"),
                 "sequence", "protein_mass")\
            .withColumn("protein_altName_fullName",coalesce("protein_altName_fullName_0","protein_altName_fullName_1"))\
            .withColumn("protein_altName_shortName",coalesce("protein_altName_shortName_0","protein_altName_shortName_1"))\
            .select("id","primary_accession","sec_accession_1","sec_accession_2","sec_accession_3",
                 "primary_gene_name", "sec_gene_name_1", "sec_gene_name_2",
                 "primary_protein_fullname","other_protein_rec_fullname",
                 "protein_rec_shortname", "other_protein_rec_shortname", 
                 "protein_altName_fullName","protein_altName_shortName",
                 "sequence", "protein_mass")
                 
df7.printSchema()
df7.count()

#Write the final dfs into a single csv file on s3
df7.coalesce(1).write.format('csv').option('header','true').mode("append").save("s3://data-pipeline-bucket/abcam/outputs_slice")

job.commit()