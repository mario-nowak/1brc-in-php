<?php

namespace MarioNowak\obrcInPhp;

require __DIR__ . '/../vendor/autoload.php';

use Spatie\Fork\Fork;


function readFile(string $fileName, int $startingByte, int $numberBytesToRead, int $processIndex)
{
    print_r("Starting process $processIndex at $startingByte\n");
    $file = fopen($fileName, "r") or die("Unable to open file!\n");
    // Move to starting byte
    fseek($file, $startingByte);

    // Move back until a \n is encountered
    if ($startingByte !== 0) {
        $startPos = $startingByte;
        while ($startPos > 0) {
            fseek($file, --$startPos);
            $char = fgetc($file);

            if ($char === "\n") {
                // Move the file pointer to the position after the newline
                fseek($file, $startPos + 1);
                break;
            }
        }
    }

    //
    $weatherStationData = [];
    $processedRows = 0;

    $currentByte = ftell($file);
    while ($currentByte < ($startingByte + $numberBytesToRead)) {
        $content = fgets($file);
        if (!$content) {
            break;
        }

        $currentByte = ftell($file);
        if ($currentByte > ($startingByte + $numberBytesToRead)) {
            break;
        }

        $processedRows += 1;

        // use content
        [$station, $temperature] = explode(";", $content);
        $temperature = (float) $temperature;
//        if ($station === 'Oster') {
//            print_r("$temperature $processIndex\n");
//        }

        // append it
        if (array_key_exists($station, $weatherStationData)) {
            $weatherStationData[$station] = [
                'min' => min($weatherStationData[$station]['min'], $temperature),
                'max' => max($weatherStationData[$station]['max'], $temperature),
                'sum' => $weatherStationData[$station]['sum'] + $temperature,
                'num' => $weatherStationData[$station]['num'] += 1
            ];
        } else {
            $weatherStationData[$station] = [
                'min' => $temperature,
                'max' => $temperature,
                'sum' => $temperature,
                'num' => 1
            ];
        }
    }
    fclose($file);

    print_r("Process $processIndex processed $processedRows rows\n");

    file_put_contents("result_$processIndex.json", json_encode($weatherStationData));
}

function main()
{
    $startTime = microtime(true);
    $fileName = "src/measurements.txt";
    $fileSize = filesize($fileName);
    print_r("The file size is $fileSize\n");
    $NUM_PROCESSES = 8;
    $fileSizePerProcess = floor($fileSize / $NUM_PROCESSES);

    $tasks = array_map(
        function($index) use ($fileName, $fileSizePerProcess) {
            return function () use ($index, $fileName, $fileSizePerProcess) {
                $content = readFile(
                    $fileName,
                    $fileSizePerProcess * $index,
                    $fileSizePerProcess + 1,
                    $index
                );

                return $content;
            };
        },
        range(0, $NUM_PROCESSES - 1)
    );

    Fork::new()->run(...$tasks);

    $weatherStationData = [];
    for ($processIndex = 0; $processIndex < $NUM_PROCESSES; $processIndex += 1) {
        $weatherStationDataOfProcess = json_decode(
            file_get_contents("result_$processIndex.json"),
            true
        );
//        $processedRows = count($weatherStationDataOfProcess);
//        var_dump("Process $processIndex created $processedRows rows");
        foreach ($weatherStationDataOfProcess as $stationName => $metricsOfStation) {
            if (array_key_exists($stationName, $weatherStationData)) {
                $weatherStationData[$stationName] = [
                    'min' => min($weatherStationData[$stationName]['min'], $metricsOfStation['min']),
                    'max' => max($weatherStationData[$stationName]['max'], $metricsOfStation['max']),
                    'sum' => $weatherStationData[$stationName]['sum'] + $metricsOfStation['sum'],
                    'num' => $weatherStationData[$stationName]['num'] += $metricsOfStation['num']
                ];
            } else {
                $weatherStationData[$stationName] = [
                    'min' => $metricsOfStation['min'],
                    'max' => $metricsOfStation['max'],
                    'sum' => $metricsOfStation['sum'],
                    'num' => $metricsOfStation['num']
                ];
            }
        }

        unlink("result_$processIndex.json");
    }

    foreach ($weatherStationData as &$stationMetrics) {
        $stationMetrics['average'] = $stationMetrics['sum'] / $stationMetrics['num'];
        unset($stationMetrics['sum']);
        unset($stationMetrics['num']);
    }
    $endTime = microtime(true);

    $totalTime = $endTime - $startTime;
    print_r("Took $totalTime seconds\n");

    var_dump(count($weatherStationData));
//    var_dump($weatherStationData['Minneapolis']);
}

main();
