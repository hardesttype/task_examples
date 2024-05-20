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

DECLARE
  roc_auc_value NUMBER;
BEGIN
  SELECT ComputeRocAuc(predicted_score, actual_class) INTO roc_auc_value
  FROM model_predictions;

  DBMS_OUTPUT.PUT_LINE('ROC-AUC Value: ' || roc_auc_value);
END;

-----------------------------------------------------------------
-- VERSION 3
CREATE OR REPLACE TYPE RocAucImpl AS OBJECT (
  positive_rank_sum NUMBER,
  positive_count    NUMBER,
  negative_count    NUMBER,

  STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT RocAucImpl) RETURN NUMBER,
  MEMBER FUNCTION ODCIAggregateIterate(self IN OUT RocAucImpl, value IN NUMBER, label IN NUMBER) RETURN NUMBER,
  MEMBER FUNCTION ODCIAggregateTerminate(self IN RocAucImpl, returnValue OUT NUMBER, flags IN NUMBER) RETURN NUMBER,
  MEMBER FUNCTION ODCIAggregateMerge(self IN OUT RocAucImpl, ctx2 IN RocAucImpl) RETURN NUMBER
);
/

CREATE OR REPLACE TYPE BODY RocAucImpl IS

STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT RocAucImpl) RETURN NUMBER IS
BEGIN
  sctx := RocAucImpl(0, 0, 0);
  RETURN ODCIConst.Success;
END;

MEMBER FUNCTION ODCIAggregateIterate(self IN OUT RocAucImpl, value IN NUMBER, label IN NUMBER) RETURN NUMBER IS
BEGIN
  IF label = 1 THEN
    self.positive_rank_sum := self.positive_rank_sum + value;
    self.positive_count := self.positive_count + 1;
  ELSE
    self.negative_count := self.negative_count + 1;
  END IF;
  RETURN ODCIConst.Success;
END;

MEMBER FUNCTION ODCIAggregateTerminate(self IN RocAucImpl, returnValue OUT NUMBER, flags IN NUMBER) RETURN NUMBER IS
BEGIN
  -- Formula for AUC: (Sum of ranks for positive samples - (positive_count * (positive_count + 1)/2)) / (positive_count * negative_count)
  returnValue := (self.positive_rank_sum - (self.positive_count * (self.positive_count + 1) / 2)) / (self.positive_count * self.negative_count);
  RETURN ODCIConst.Success;
END;

MEMBER FUNCTION ODCIAggregateMerge(self IN OUT RocAucImpl, ctx2 IN RocAucImpl) RETURN NUMBER IS
BEGIN
  self.positive_rank_sum := self.positive_rank_sum + ctx2.positive_rank_sum;
  self.positive_count := self.positive_count + ctx2.positive_count;
  self.negative_count := self.negative_count + ctx2.negative_count;
  RETURN ODCIConst.Success;
END;

END;
/

-----------------------------------------------------------------
-- VERSION 1 (CLAUDE)
CREATE OR REPLACE TYPE score_label_pair AS OBJECT (
  score NUMBER,
  label NUMBER
);
/

CREATE OR REPLACE TYPE score_label_tab IS TABLE OF score_label_pair;
/

CREATE OR REPLACE FUNCTION roc_auc(scores_labels score_label_tab) RETURN NUMBER IS
  total_pos NUMBER := 0;
  total_neg NUMBER := 0;
  sum_ranks NUMBER := 0;
  current_rank NUMBER := 0;
  current_score NUMBER := -1;
  tie_count NUMBER := 0;
BEGIN
  -- Count total positives and negatives
  FOR i IN 1..scores_labels.COUNT LOOP
    IF scores_labels(i).label = 1 THEN
      total_pos := total_pos + 1;
    ELSE
      total_neg := total_neg + 1;
    END IF;
  END LOOP;

  -- Sort scores in descending order
  scores_labels := score_label_tab(
    SELECT * FROM TABLE(scores_labels)
    ORDER BY score DESC
  );

  -- Compute sum of ranks for positive instances
  FOR i IN 1..scores_labels.COUNT LOOP
    IF scores_labels(i).score != current_score THEN
      current_rank := current_rank + tie_count;
      tie_count := 1;
      current_score := scores_labels(i).score;
    ELSE
      tie_count := tie_count + 1;
    END IF;

    IF scores_labels(i).label = 1 THEN
      sum_ranks := sum_ranks + current_rank;
    END IF;
  END LOOP;

  -- Compute AUC
  RETURN (sum_ranks - (total_pos * (total_pos + 1) / 2)) / (total_pos * total_neg);
END;
/
CREATE FUNCTION RocAuc (value NUMBER, label NUMBER) RETURN NUMBER 
PARALLEL_ENABLE AGGREGATE USING RocAucImpl;

-----------------------------------------------------------------
-- VERSION 2 (CLAUDE)
CREATE OR REPLACE TYPE ROC_AUC_Aggregate AS OBJECT (
  -- Attributes to store aggregation context
  sum_pos NUMBER,
  sum_neg NUMBER,
  total_pos NUMBER, 
  total_neg NUMBER,
  
  -- ODCIAggregate interface methods  
  STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT ROC_AUC_Aggregate) RETURN NUMBER,
  
  MEMBER FUNCTION ODCIAggregateIterate(self IN OUT ROC_AUC_Aggregate, 
                                       value NUMBER, label NUMBER) RETURN NUMBER,
                                       
  MEMBER FUNCTION ODCIAggregateMerge(self IN OUT ROC_AUC_Aggregate,
                                     ctx2 IN ROC_AUC_Aggregate) RETURN NUMBER,
                                      
  MEMBER FUNCTION ODCIAggregateTerminate(self IN ROC_AUC_Aggregate,
                                         returnValue OUT NUMBER, 
                                         flags IN NUMBER) RETURN NUMBER
);
/

CREATE OR REPLACE TYPE BODY ROC_AUC_Aggregate IS

  STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT ROC_AUC_Aggregate) 
  RETURN NUMBER IS
  BEGIN
    sctx := ROC_AUC_Aggregate(0, 0, 0, 0);
    RETURN ODCIConst.Success;
  END;

  MEMBER FUNCTION ODCIAggregateIterate(self IN OUT ROC_AUC_Aggregate, 
                                       value NUMBER, label NUMBER) 
  RETURN NUMBER IS
  BEGIN
    IF label = 1 THEN
      self.sum_pos := self.sum_pos + value;
      self.total_pos := self.total_pos + 1;
    ELSE 
      self.sum_neg := self.sum_neg + value;
      self.total_neg := self.total_neg + 1;
    END IF;
    RETURN ODCIConst.Success;
  END;
  
  MEMBER FUNCTION ODCIAggregateMerge(self IN OUT ROC_AUC_Aggregate,
                                     ctx2 IN ROC_AUC_Aggregate) 
  RETURN NUMBER IS
  BEGIN
    self.sum_pos := self.sum_pos + ctx2.sum_pos;
    self.sum_neg := self.sum_neg + ctx2.sum_neg;
    self.total_pos := self.total_pos + ctx2.total_pos; 
    self.total_neg := self.total_neg + ctx2.total_neg;
    RETURN ODCIConst.Success;
  END;

  MEMBER FUNCTION ODCIAggregateTerminate(self IN ROC_AUC_Aggregate,
                                         returnValue OUT NUMBER,
                                         flags IN NUMBER) 
  RETURN NUMBER IS
    auc NUMBER;
  BEGIN
    auc := (self.sum_pos/self.total_pos - (self.total_pos+1)/2) / self.total_neg;
    returnValue := auc;
    RETURN ODCIConst.Success;
  END;

END;
/

CREATE OR REPLACE FUNCTION ROC_AUC(score NUMBER, label NUMBER) RETURN NUMBER
PARALLEL_ENABLE AGGREGATE USING ROC_AUC_Aggregate;
