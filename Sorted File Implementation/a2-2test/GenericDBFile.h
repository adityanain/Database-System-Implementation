#ifndef GENERIC_DBFILE_H
#define GENERIC_DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

class GenericDBFile{
 public:
  GenericDBFile ();
  virtual ~GenericDBFile() = 0;
  virtual int Create (char *fpath, fType file_type, void *startup) = 0;
  virtual int Open (char *fpath) = 0;
  virtual int Close () = 0;

  virtual void Load (Schema &myschema, char *loadpath) = 0;

  virtual void MoveFirst () = 0;
  virtual void Add (Record &addme) = 0;
  virtual int GetNext (Record &fetchme) = 0;
  virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
};
#endif
