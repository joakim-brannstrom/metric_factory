all: sqlite3.o
	cp sqlite3.o build/

sqlite3.o: c/sqlite3.c
	gcc -c -O2 -DSQLITE_ENABLE_COLUMN_METADATA c/sqlite3.c -o sqlite3.o

clean:
	rm -f *.o
