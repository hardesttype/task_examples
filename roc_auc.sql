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
