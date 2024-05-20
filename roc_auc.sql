-----------------------------------------------------------------
-- VERSION 1
-- Create a new type to hold the intermediate results
CREATE OR REPLACE TYPE RocAucType AS OBJECT (
    pos_samples NUMBER,
    neg_samples NUMBER,
    sorted_list  SYS.ODCINUMBERLIST,
    STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT RocAucType) RETURN NUMBER,
    MEMBER FUNCTION ODCIAggregateIterate(self IN OUT RocAucType, value IN NUMBER, class IN NUMBER) RETURN NUMBER,
    MEMBER FUNCTION ODCIAggregateMerge(self IN OUT RocAucType, ctx2 IN RocAucType) RETURN NUMBER,
    MEMBER FUNCTION ODCIAggregateTerminate(self IN RocAucType, returnValue OUT NUMBER, flags IN NUMBER) RETURN NUMBER
);
/

-- Create the body for the type
CREATE OR REPLACE TYPE BODY RocAucType IS 
    STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT RocAucType) RETURN NUMBER IS
    BEGIN
        sctx := RocAucType(0, 0, SYS.ODCINUMBERLIST());
        RETURN ODCIConst.Success;
    END;

    MEMBER FUNCTION ODCIAggregateIterate(self IN OUT RocAucType, value IN NUMBER, class IN NUMBER) RETURN NUMBER IS
    BEGIN
        IF class = 1 THEN
            self.pos_samples := self.pos_samples + 1;
        ELSE
            self.neg_samples := self.neg_samples + 1;
        END IF;
        -- Add value to sorted_list (Note: actual sorting should be done in ODCIAggregateTerminate)
        self.sorted_list.EXTEND;
        self.sorted_list(self.sorted_list.LAST) := value;
        RETURN ODCIConst.Success;
    END;

    MEMBER FUNCTION ODCIAggregateMerge(self IN OUT RocAucType, ctx2 IN RocAucType) RETURN NUMBER IS
    BEGIN
        self.pos_samples := self.pos_samples + ctx2.pos_samples;
        self.neg_samples := self.neg_samples + ctx2.neg_samples;
        self.sorted_list := self.sorted_list MULTISET UNION ALL ctx2.sorted_list;
        RETURN ODCIConst.Success;
    END;

    MEMBER FUNCTION ODCIAggregateTerminate(self IN RocAucType, returnValue OUT NUMBER, flags IN NUMBER) RETURN NUMBER IS
        tp NUMBER := 0;
        fp NUMBER := 0;
        auc NUMBER := 0;
        prev_val NUMBER := -1;
    BEGIN
        -- Sort the list
        self.sorted_list := self.sorted_list.MULTISET_SORT();

        -- Calculate AUC
        FOR i IN 1 .. self.sorted_list.COUNT LOOP
            IF self.sorted_list(i) != prev_val THEN
                auc := auc + (tp/self.pos_samples) * (fp/self.neg_samples);
                prev_val := self.sorted_list(i);
                tp := 0;
                fp := 0;
            END IF;
            IF -- condition to check if current sample is positive -- THEN
                tp := tp + 1;
            ELSE
                fp := fp + 1;
            END IF;
        END LOOP;
        auc := auc + (tp/self.pos_samples) * (fp/self.neg_samples);

        returnValue := auc;
        RETURN ODCIConst.Success;
    END;
END;
/

-- Create the aggregate function
CREATE OR REPLACE FUNCTION ComputeRocAuc (input NUMBER, class NUMBER)
RETURN NUMBER
PARALLEL_ENABLE AGGREGATE USING RocAucType;
/

-----------------------------------------------------------------
-- VERSION 2
-- Define the type for the custom aggregate function
CREATE OR REPLACE TYPE RocAucType AS OBJECT (
  positive_count NUMBER,
  negative_count NUMBER,
  scores SYS.ODCIVARCHAR2LIST,
  classes SYS.ODCINUMBERLIST,
  STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT RocAucType) RETURN NUMBER,
  MEMBER FUNCTION ODCIAggregateIterate(self IN OUT RocAucType, score IN VARCHAR2, class IN NUMBER) RETURN NUMBER,
  MEMBER FUNCTION ODCIAggregateMerge(self IN OUT RocAucType, ctx2 IN RocAucType) RETURN NUMBER,
  MEMBER FUNCTION ODCIAggregateTerminate(self IN RocAucType, returnValue OUT NUMBER, flags IN NUMBER) RETURN NUMBER
);
/

-- Define the body for the type
CREATE OR REPLACE TYPE BODY RocAucType IS
  STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT RocAucType) RETURN NUMBER IS
  BEGIN
    sctx := RocAucType(0, 0, SYS.ODCIVARCHAR2LIST(), SYS.ODCINUMBERLIST());
    RETURN ODCIConst.Success;
  END;

  MEMBER FUNCTION ODCIAggregateIterate(self IN OUT RocAucType, score IN VARCHAR2, class IN NUMBER) RETURN NUMBER IS
  BEGIN
    IF class = 1 THEN
      self.positive_count := self.positive_count + 1;
    ELSE
      self.negative_count := self.negative_count + 1;
    END IF;
    self.scores.EXTEND;
    self.scores(self.scores.LAST) := score;
    self.classes.EXTEND;
    self.classes(self.classes.LAST) := class;
    RETURN ODCIConst.Success;
  END;

  MEMBER FUNCTION ODCIAggregateMerge(self IN OUT RocAucType, ctx2 IN RocAucType) RETURN NUMBER IS
  BEGIN
    self.positive_count := self.positive_count + ctx2.positive_count;
    self.negative_count := self.negative_count + ctx2.negative_count;
    self.scores := self.scores MULTISET UNION ALL ctx2.scores;
    self.classes := self.classes MULTISET UNION ALL ctx2.classes;
    RETURN ODCIConst.Success;
  END;

  MEMBER FUNCTION ODCIAggregateTerminate(self IN RocAucType, returnValue OUT NUMBER, flags IN NUMBER) RETURN NUMBER IS
    -- The actual logic to compute ROC-AUC will go here.
    -- This is a placeholder to illustrate the method signature.
    -- The computation of the ROC-AUC is non-trivial and would
    -- require implementation of the algorithm in PL/SQL.
    BEGIN
      returnValue := 0; -- Placeholder result
      RETURN ODCIConst.Success;
    END;
END;
/

-- Create the custom aggregate function
CREATE OR REPLACE FUNCTION ComputeRocAuc (
  score IN VARCHAR2,
  class IN NUMBER
) RETURN NUMBER
AGGREGATE USING RocAucType;
/
