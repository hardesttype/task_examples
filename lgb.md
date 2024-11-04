import lightgbm as lgb
import optuna

lgb_train = lgb.Dataset(
    X_train, 
    y_train,
    free_raw_data = True,
    # feature_name = feature_names,
    # categorical_feature=category_features
)
lgb_test = lgb.Dataset(
    X_test, 
    y_test,
    free_raw_data = False,
)

best_metric = 0

def objective(trial):
    global best_metric
    
    param = {
        'objective': 'binary',
        'force_col_wise': True,
        'num_threads': 16,
        'boosting': 'goss',
        'feature_pre_filter': False,
        'learning_rate': trial.suggest_float('learning_rate', 0.001, 0.5, log=True),
        'lambda_l1': trial.suggest_float('lambda_l1', 1e-8, 10.0, log=True),
        'lambda_l2': trial.suggest_float('lambda_l2', 1e-8, 10.0, log=True),
        'num_leaves': trial.suggest_int('num_leaves', 10, 64),
        'feature_fraction': trial.suggest_float('feature_fraction', 0.4, 1.0),
        # 'bagging_fraction': trial.suggest_float('bagging_fraction', 0.4, 1.0),
        # 'bagging_freq': trial.suggest_int('bagging_freq', 2, 10),
        'max_depth': trial.suggest_int('max_depth', 4, 10),
        'min_sum_hessian_in_leaf': trial.suggest_float('min_sum_hessian_in_leaf', 1e-8, 10.0, log=True),
        'min_child_samples': trial.suggest_int('min_child_samples', 20, 200),
    }
 
    gbm = lgb.train(
        param,
        lgb_train,
        num_boost_round=1000,
        valid_sets=lgb_test,
        early_stopping_rounds=200,
        verbose_eval=1,
    )
    
    preds = gbm.predict(lgb_test.data)
    roc_score = roc_auc_score(lgb_test.label, preds, multi_class='ovr')
    
    if roc_score > best_metric:
        gbm.save_model(f'model.txt')
        best_metric = roc_score
    
    return 100 * (2 * roc_score - 1)
 
study = optuna.create_study(direction='maximize')
study.optimize(objective, n_trials=32)

print('Number of finished trials:', len(study.trials))
print('Best trial:', study.best_trial.params)
