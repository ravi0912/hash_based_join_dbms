import sys
from collections import defaultdict

#maxroud = maximum number of hashing rounds
global r1_size, r2_size, page_size, availablepages, maxround, r1_per_page, r2_per_page, noofbucket
r1 = []
r2 = []
match = []

def upperbound(a, b):
	if (a%b == 0):
		return a/b
	else:
		return a/b +1

# n = round counting of hash function,
# m = no. of bucket
# a = record
def hashfunc(a, n, m):
	bin = []
	p=a
	n = 4+n-1
	for i in range(0,n):
		bin.append(a%2)
		a=int(a/2)

	t2=1
	x2=0	
	for i in range(0,n):
		t2 = t2*2
		x2 = x2 + bin[i]*t2

	#print "test", x2/2, "a", p
	return (x2/2)%m

def func(b1,b2,roundcount,bucketno):
	a = len(b1)
	b = len(b2)

	if(a==0 or b ==0):
		print >> f, "No matching tuple found!! No further processing required"

	p1 = upperbound(a,r1_per_page)
	p2 = upperbound(b,r2_per_page)

	flag=0
	if((min(p1,p2)+2) <= availablepages):
		print >> f, 'Bucket %d: Total size is: %d' % (bucketno,p1+p2)
		print >> f, "Available pages: ", availablepages
		print >> f, "Performing in memory join."
		print >> f, "Matching pairs are: "
		for p in range(0,a):
			for q in range(0,b):
				if(b1[p] == b2[q]):
					flag = 1
					print >> f, b1[p], " "
					match.append(b1[p])

		if(flag!=1):
			print >> f, "No matching pair found!!!"

	else:
		roundcount+=1
		if(roundcount<=maxround):
			noofbucket = availablepages-1
			print >> f, "Bucket", bucketno , ": Total size is: ", p1+p2
			print >> f, "Available pages: ", availablepages
			print >> f, "Cannot perform in memory join."
			print >> f, "Performing round ", roundcount, " of hashing."

			print >> f, "Size of relation 1: ", upperbound(a,r1_per_page), " pages."
			print >> f, "Size of relation 2: ", upperbound(b,r2_per_page), " pages."
			print >> f, "Total number of available pages: ", availablepages
			print >> f, "Number of buckets in hash table: ", noofbucket

			buck1 = defaultdict(list)
			buck2 = defaultdict(list)

			print >> f, "\nHashing Round", roundcount, ": "
			print >> f, "Reading relation1:"

			for i in range(0,a):	
				hval = hashfunc(b1[i],roundcount,noofbucket)
				buck1[hval].append(b1[i])
				print >> f, "Tuple ", i+1, ": ", b1[i], " Mapped to bucket ", hval
				if(len(buck1[hval])%r1_per_page == 0):
					print >> f, "Page for bucket ", hval, " full. Flushed to secondary storage"

			print >> f, "Reading relation2:"
			

			for i in range(0,b):	
				hval = hashfunc(b2[i],roundcount,noofbucket)
				buck2[hval].append(b2[i])
				print >> f, "Tuple ", i+1, ": ", b2[i], " Mapped to bucket ", hval
				if(len(buck2[hval])%r2_per_page == 0):
					print >> f, "Page for bucket ", hval, " full. Flushed to secondary storage"

			for j in range(0,noofbucket):
				func(buck1[j],buck2[j],roundcount,j+1)
		else:
			print >> f, "Maxround of hashing exceeded. Exiting!!!"
			sys.exit()


#taking inpuut from terminal
file1 = sys.argv[1]
file2 = sys.argv[2]
r1_size = int(sys.argv[3])
r2_size = int(sys.argv[4])
page_size = int(sys.argv[5])
availablepages = int(sys.argv[6])
maxround = int(sys.argv[7])
#calculating per page accomodate size
r1_per_page = page_size/r1_size
r2_per_page = page_size/r2_size
f = open("out.txt",'w')

f1 = open(file1).readlines()
for line in f1:
	line = line.strip('\n')
	r1.append(int(line))

	f2 = open(file2).readlines()
for line in f2:
	line = line.strip('\n')
	r2.append(int(line))

# available page left for write back because each bucket takes one page
noofbucket = availablepages - 1

print >> f, "Output:" 
print >> f, "Total no. of available pages: ", availablepages
print >> f, "# buckets in hash table: ", noofbucket
print >> f, "Size of rel. 1: ", upperbound(len(r1),r1_per_page), " pages."
print >> f, "Size of rel. 2: ", upperbound(len(r2),r2_per_page), " pages."

b1 = defaultdict(list)
b2 = defaultdict(list)
print >> f, "Hashing Round number 1: "
print >> f, "Reading rel. 1:"

a = len(r1)

for i in range(0,a):	
	hval = hashfunc(r1[i],1,noofbucket)
	b1[hval].append(r1[i])
	print >> f, "Tuple ", i+1, ": ", r1[i], " Mapped to bucket ", hval
	if(len(b1[hval])%r1_per_page == 0):
		print >> f, "Bucket Page :  ", hval, " full. Flushed to secondary storage"
print >> f, "Relation1 over.\n"

print >> f, "\nReading relation2:"

b = len(r2) 

for i in range(0,b):	
	hval = hashfunc(r2[i],1,noofbucket)
	b2[hval].append(r2[i])
	print >> f, "Tuple ", i+1, ": ", r2[i], " Mapped to bucket ", hval
	# print >> f, len(b1[hval])
	if(len(b2[hval])%r2_per_page == 0):
		print >> f, "Bucket Page : ", hval, " full. Flushed to secondary storage"
print >> f, "Relation2 over.\n"

print >> f, "\n-------------printing no of pages in each bucket for both list--------------\n"

print >> f, "List b1: \n"
for i in range(0,noofbucket):
	print >> f, "No of pages in bucket ", i+1, ": ", upperbound(len(b1[i]),r1_per_page)


print >> f, "\nList b2: \n"
for i in range(0,noofbucket):
	print >> f, "No of pages in bucket ", i+1, ": ", upperbound(len(b2[i]),r2_per_page)

for i in range(0,noofbucket):
	print >> f,"\nMain Bucket : \n",i+1
	func(b1[i],b2[i],1,i+1)

print >> f,"\nMatching pair found are: ", set(match)

f.close()