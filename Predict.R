library(data.table)
library(lightgbm)

GetMax = function(x)
{
	max(c(x, -Inf), na.rm = T)	
}

GetMin = function(x)
{
	min(c(x, Inf), na.rm = T)	
}


GetColumnData = function(x, di)
{
	max_di = GetMax(di)
	list(
		x[1]
		, x[1] - x[2]
		, x[1] - mean(x[di >= max_di - 7], na.rm = T)
		, x[1] - mean(x[di >= max_di - 14], na.rm = T)
		, x[1] - mean(x[di >= max_di - 21], na.rm = T)

		, GetMin(x[di >= max_di - 7])
		, GetMax(x[di >= max_di - 7])
		, GetMax(x[di >= max_di - 7]) - GetMin(x[di >= max_di - 7])
		, mean(x[di >= max_di - 7], na.rm = T)

		, GetMin(x[di >= max_di - 14])
		, GetMax(x[di >= max_di - 14])
		, GetMax(x[di >= max_di - 14]) - GetMin(x[di >= max_di - 14])
		, mean(x[di >= max_di - 14],, na.rm = T)

		, GetMin(x[di >= max_di - 21])
		, GetMax(x[di >= max_di - 21])
		, GetMax(x[di >= max_di - 21]) - GetMin(x[di >= max_di - 21])
		, mean(x[di >= max_di - 21],, na.rm = T)
	)
}

GetColumn2ndData = function(x, di)
{
	max_di = GetMax(di)
	rencent_x = x[di >= max_di - 21]
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

GetColumn3rdData = function(x, di)
{
	max_di = GetMax(di)
	list(
		x[1]
		, x[1] - x[2]
		, x[1] - mean(x[di >= max_di - 7], na.rm = T)
		, x[1] - mean(x[di >= max_di - 14], na.rm = T)
		, x[1] - mean(x[di >= max_di - 21], na.rm = T)
	)
}

GetData = function(x_features)
{
	x_features = x_features[order(-Di)]

	data.table(x_features[, c(
		smart_3raw[1]
		, GetColumnData(smart_4raw, Di)
		, GetColumnData(smart_5raw, Di)
		, GetColumnData(smart_7raw, Di)
		, GetColumn2ndData(smart_7raw, Di)

		, GetColumn3rdData(smart_12raw, Di)
		, GetColumn3rdData(smart_184raw, Di)
		, GetColumn3rdData(smart_187raw, Di)
		, GetColumn3rdData(smart_189raw, Di)
		, GetColumn3rdData(smart_190raw, Di)
		, GetColumn3rdData(smart_191raw, Di)
		, GetColumn3rdData(smart_192raw, Di)
		, GetColumn3rdData(smart_193raw, Di)
		, GetColumn3rdData(smart_194raw, Di)
		, GetColumn3rdData(smart_197raw, Di)
		, GetColumn3rdData(smart_198raw, Di)
		, GetColumn3rdData(smart_199raw, Di)
		, smart_240_normalized[1]
		, smart_240_normalized[1] - smart_240_normalized[2]
	), .(SerialNumber, Label, PredictionDi, Model)])
}

BModels = list()
for (a in 1:56)
{
	BModels = c(BModels, lgb.load(paste0("Models/b", a)))
}
cat(paste(length(BModels), "BModels.\n"))


SelectedNumberA = c(240)
SelectedNumberB = c(1, 3, 4, 5, 7,  9, 10, 12, 184, 187, 188, 189, 190, 191, 192, 193, 194, 197, 198, 199)
SelectedColumns = c(c(1, 2, 3, 514), 2 + 2 * SelectedNumberA, 3 + 2 * SelectedNumberB)
TestingDi = 0
TestingLogs = NULL
for (a in 20180811:20180831)
{
	testing_logs = fread(paste0("/tcdata/disk_sample_smart_log_round2/disk_sample_smart_log_", a, "_round2.csv"), select = SelectedColumns)
	names(testing_logs)[1:3] = c("SerialNumber", "Manufacturer", "Model")
	testing_logs$Di = as.double(as.Date(as.character(testing_logs$dt), "%Y%m%d") - as.Date("20180901", "%Y%m%d"))
	
	TestingLogs = rbind(TestingLogs, testing_logs)
}

PredictedSerialNumbers = NULL
CurrentDi = 0
Predictions = NULL
NPredictions = rep(4, 30)
for (a in 20180901:20180930)
{
	print(a)
	
	testing_logs = fread(paste0("/tcdata/disk_sample_smart_log_round2/disk_sample_smart_log_", a, "_round2.csv"), select = SelectedColumns)
	names(testing_logs)[1:3] = c("SerialNumber", "Manufacturer", "Model")
	testing_logs$Di = as.double(as.Date(as.character(testing_logs$dt), "%Y%m%d") - as.Date("20180901", "%Y%m%d"))
	testing_serial_numbers = setdiff(testing_logs$SerialNumber, PredictedSerialNumbers)

	{
		TestingLogs = rbind(TestingLogs, testing_logs)

		testing_labels = TestingLogs[SerialNumber %in% testing_serial_numbers, .(SerialNumber, Model)]
		testing_labels = testing_labels[, .(PredictionDi = CurrentDi, Label = -1), .(SerialNumber, Model)]
		testing_features = merge(testing_labels, TestingLogs, by = c("SerialNumber", "Model"))
		testing_data = GetData(testing_features)
		testing_matrix = data.matrix(testing_data[, c(-1:-4)])
		
		if (CurrentDi == 0)
			cat(paste(ncol(testing_matrix), "Features.\n"))
		
		lgb_predictions = NULL
		for (b in 1:length(BModels))
		{
			lgb_predictions = rbind(lgb_predictions, testing_data[, .(SerialNumber, Model, PredictionDi, PredictionScore = predict(BModels[[b]], testing_matrix))])
		}
		lgb_predictions = lgb_predictions[, .(PredictionScore = sum(PredictionScore)), .(SerialNumber, Model, PredictionDi)]
		
		predictions = NULL
		if (NPredictions[1 + CurrentDi] > 0)
		{
			predictions = rbind(predictions, lgb_predictions[order(-PredictionScore)][1:min(nrow(lgb_predictions), NPredictions[1 + CurrentDi])])
		}
		Predictions = rbind(Predictions, predictions)
		PredictedSerialNumbers = union(PredictedSerialNumbers, predictions$SerialNumber)
	}

	CurrentDi = 1 + CurrentDi
}

cat(paste(nrow(Predictions), "Predictions.\n"))
print(Predictions[, .(PredictionDi = min(PredictionDi)), .(Model, SerialNumber)][, .(N = .N), .(PredictionDi)])

write.table(Predictions[, .(Manufacturer = "A", Model, SerialNumber, Dt = as.character(as.Date("2018-09-01") + PredictionDi))], "result.csv", sep=",", row.names = F, col.names = F, quote = F)

