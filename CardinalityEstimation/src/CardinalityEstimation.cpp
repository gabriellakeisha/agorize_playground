#include <common/Root.h>
#include <CardinalityEstimation.h>
#include <stdexcept>  // For exception handling

// **Constructor Implementation**
CEEngine::CEEngine(int num, DataExecuter *dataExecuter)
    : countMinA(std::ceil(num * 0.01), 5),
      countMinB(std::ceil(num * 0.01), 5),
      sampler(0.1) {
    this->dataExecuter = dataExecuter;
    prepare();
}

// **insertTuple Method Implementation**
void CEEngine::insertTuple(const std::vector<int>& tuple) {
    if (tuple.size() < 2) {
        throw std::invalid_argument("Tuple must contain at least two elements.");
    }

    int valueA = tuple[0];  // Extract the first column value
    int valueB = tuple[1];  // Extract the second column value

    if (sampler.shouldSample()) {   // Check if the tuple should be sampled
        countMinA.add(valueA);      // Add value to CountMinSketch for column A
        countMinB.add(valueB);      // Add value to CountMinSketch for column B
    }
}

// **deleteTuple Method Implementation**
void CEEngine::deleteTuple(const std::vector<int>& tuple, int tupleId) {
    if (tuple.size() < 2) {
        throw std::invalid_argument("Tuple must contain at least two elements.");
    }

    int valueA = tuple[0];  // Extract the first column value
    int valueB = tuple[1];  // Extract the second column value

    countMinA.remove(valueA);  // Remove value from CountMinSketch for column A
    countMinB.remove(valueB);  // Remove value from CountMinSketch for column B
}

// **query Method Implementation**
int CEEngine::query(const std::vector<CompareExpression>& quals) {
    if (quals.empty()) {
        throw std::invalid_argument("Query conditions cannot be empty.");
    }

    std::unordered_map<int, int> columnEstimates;

    // Process each query condition
    for (const auto& qual : quals) {
        int estimate = (qual.columnIdx == 0)
                           ? countMinA.estimate(qual.value)
                           : countMinB.estimate(qual.value);

        // Update the column estimates with the minimum value seen so far
        if (columnEstimates.find(qual.columnIdx) == columnEstimates.end()) {
            columnEstimates[qual.columnIdx] = estimate;
        } else {
            columnEstimates[qual.columnIdx] = std::min(columnEstimates[qual.columnIdx], estimate);
        }
    }

    // Compute the final result as the minimum across all column estimates
    int finalResult = INT_MAX;
    for (const auto& pair : columnEstimates) {
        finalResult = std::min(finalResult, pair.second);
    }

    return finalResult;
}

// **prepare Method Implementation**
void CEEngine::prepare() {
    // Reset CountMinSketch tables to initial states
    countMinA = CountMinSketch(countMinA.getWidth(), countMinA.getDepth());
    countMinB = CountMinSketch(countMinB.getWidth(), countMinB.getDepth());
}
