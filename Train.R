library(data.table)
library(lightgbm)

SelectedNumberA = c(240)
SelectedNumberB = c(1, 3, 4, 5, 7,  9, 10, 12, 184, 187, 188, 189, 190, 191, 192, 193, 194,  197, 198,  199)
SelectedColumns = 1 + c(c(1, 2, 3, 514), 2 2 * SelectedNumberA, 3 2 * SelectedNumberB)
TrainingLogs = NULL
for  (a in c(201707:201712, 201801:201808))
{
	training_logs_a = fread(paste0("disk_sample_smart_log_", a, ".csv"), select = SelectedColumns)
	TrainingLogs = rbind(TrainingLogs, training_logs_a)
}
names(TrainingLogs)[1:3] = c("SerialNumber", "Manufacturer", "Model")
TrainingLogs$Di = as.double(as.Date(as.character(TrainingLogs$dt), "%Y%m%d") - as.Date("20180901", "%Y%m%d"))
rm(training_logs_a)
X = gc()

TrainingFaults = fread("disk_sample_fault_tag.csv", col.names = c("Manufacturer", "Model", "SerialNumber", "FaultTime", "Tag"))
TrainingFaults = rbind(TrainingFaults, fread("disk_sample_fault_tag_201808.csv", col.names = c("Manufacturer", "Model", "SerialNumber", "FaultTime", "Tag", "Key"))[, .(Manufacturer, Model, SerialNumber, FaultTime, Tag)])
TrainingFaults$FaultDi = as.double(as.Date(TrainingFaults$FaultTime, "%Y-%m-%d") - as.Date("2018-09-01", "%Y-%m-%d"))

TrainingLogs = unique(TrainingLogs, by = c("SerialNumber", "Model", "Di"))	
X = gc()

save.image()





GetMax = function(x)
{
	max(c(x, -Inf), na.rm = T)	
}

GetMin = function(x)
{
	min(c(x, Inf), na.rm = T)	
}

GetColumnData = function(x, di, prediction_di)
{
	list(
		x[1]
		, x[1] - x[2]
		, x[1] - mean(x[di >= prediction_di - 7], na.rm = T)
		, x[1] - mean(x[di >= prediction_di - 14], na.rm = T)
		, x[1] - mean(x[di >= prediction_di - 21], na.rm = T)

		, GetMin(x[di >= prediction_di - 7])
		, GetMax(x[di >= prediction_di - 7])
		, GetMax(x[di >= prediction_di - 7]) - GetMin(x[di >= prediction_di - 7])
		, mean(x[di >= prediction_di - 7], na.rm = T)

		, GetMin(x[di >= prediction_di - 14])
		, GetMax(x[di >= prediction_di - 14])
		, GetMax(x[di >= prediction_di - 14]) - GetMin(x[di >= prediction_di - 14])
		, mean(x[di >= prediction_di - 14],, na.rm = T)

		, GetMin(x[di >= prediction_di - 21])
		, GetMax(x[di >= prediction_di - 21])
		, GetMax(x[di >= prediction_di - 21]) - GetMin(x[di >= prediction_di - 21])
		, mean(x[di >= prediction_di - 21],, na.rm = T)
	)
}

GetColumn2ndData = function(x, di, prediction_di)
{
	rencent_x = x[di >= prediction_di - 21]
	if (length(rencent_x) <= 1)
		return (as.double(rep(NA, 4)))
	rencent_diff = rencent_x[2:length(rencent_x)] - rencent_x[1:(length(rencent_x) - 1)]

	list(
		GetMin(rencent_diff)
		, GetMax(rencent_diff)
		, GetMax(rencent_diff) - GetMin(rencent_diff)
		, mean(rencent_diff, na.rm = T)
	)
}

GetColumn3rdData = function(x, di, prediction_di)
{
	list(
		x[1]
		, x[1] - x[2]
		, x[1] - mean(x[di >= prediction_di - 7], na.rm = T)
		, x[1] - mean(x[di >= prediction_di - 14], na.rm = T)
		, x[1] - mean(x[di >= prediction_di - 21], na.rm = T)
	)
}

GetData = function(x_features)
{
	x_features = x_features[order(-Di)]

	data.table(x_features[, c(
		smart_3raw[1]
		, GetColumnData(smart_4raw, Di, PredictionDi)
		, GetColumnData(smart_5raw, Di, PredictionDi)
		, GetColumnData(smart_7raw, Di, PredictionDi)
		, GetColumn2ndData(smart_7raw, Di, PredictionDi)

		, GetColumn3rdData(smart_12raw, Di, PredictionDi)
		, GetColumn3rdData(smart_184raw, Di, PredictionDi)
		, GetColumn3rdData(smart_187raw, Di, PredictionDi)
		, GetColumn3rdData(smart_189raw, Di, PredictionDi)
		, GetColumn3rdData(smart_190raw, Di, PredictionDi)
		, GetColumn3rdData(smart_191raw, Di, PredictionDi)
		, GetColumn3rdData(smart_192raw, Di, PredictionDi)
		, GetColumn3rdData(smart_193raw, Di, PredictionDi)
		, GetColumn3rdData(smart_194raw, Di, PredictionDi)
		, GetColumn3rdData(smart_197raw, Di, PredictionDi)
		, GetColumn3rdData(smart_198raw, Di, PredictionDi)
		, GetColumn3rdData(smart_199raw, Di, PredictionDi)
		, smart_240_normalized[1]
		, smart_240_normalized[1] - smart_240_normalized[2]
	), .(SerialNumber, Label, PredictionDi, Model)])
}


TestingDi = 0


TrainingLogs[, Index := rank(-Di), .(SerialNumber, Model)]



Index = 1
set.seed(73)
for (b in seq(26, 305, 5))
{
	training_data = NULL
	print(paste(date(), Index))
	label_di = -b

	training_labels = TrainingLogs[, .(LabelDi = label_di, Days = sample(0:30, 1)), .(SerialNumber, Model)]
	training_features = merge(training_labels, TrainingLogs, by = c("SerialNumber", "Model"))
	training_features = training_features[Di < Days + LabelDi]
	training_features[, Index := rank(-Di), .(SerialNumber, Model)]
	training_features = training_features[Di >= label_di - 21 | Index <= 2]
	

	training_labels = training_features[Di > LabelDi, .(PredictionDi = max(Di)), .(SerialNumber, Model, LabelDi)]
	training_labels = merge(training_labels, TrainingFaults[, .(SerialNumber, Model, FaultDi)], by = c("SerialNumber", "Model"),  all.x = T, allow.cartesian = T)
	training_labels$Label = !is.na(training_labels$FaultDi) & training_labels$LabelDi >= training_labels$FaultDi - 30 & training_labels$LabelDi < training_labels$FaultDi
	training_labels = training_labels[, .(Label = max(Label)), .(SerialNumber, Model, PredictionDi)]

	training_features = merge(training_features, training_labels[, .(SerialNumber, Model, Label, PredictionDi)], by = c("SerialNumber", "Model"))

	training_data = data.table(GetData(training_features))
	training_matrix = data.matrix(training_data[, c(-1:-4)])
	cat(nrow(training_matrix), ncol(training_matrix), mean(training_data$Label), "\n")

	model = lgb.train(data = lgb.Dataset(training_matrix, label = training_data$Label), objective = "binary", nround = 300, learning_rate = 0.01, max_depth = 6, num_leaves = 127, bagging_fraction = 0.7, bagging_freq = 1, bagging_seed = 0, verbose = -1, verbosity = -1)
	lgb.save(model, paste0("Models/b", Index))

	Index = 1 + Index
}
