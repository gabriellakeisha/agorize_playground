#ifndef CARDINALITYESTIMATION_ENGINE
#define CARDINALITYESTIMATION_ENGINE

#include <vector>
#include <unordered_map>
#include <cmath>
#include <algorithm>
#include <random>
#include <climits>
#include <stdexcept>
#include <executer/DataExecuter.h>
#include <common/Expression.h>

// **Improved CountMinSketch Class**
class CountMinSketch {
private:
    std::vector<std::vector<int>> table;
    int width;
    int depth;

    // Universal hash function for better distribution
    int universalHash(int value, int i) const {
        static const int a = 31; // Some prime number
        static const int b = 17; // Another prime number
        return ((a * value + b * i) % 15485863) % width; // Large prime for modulo
    }

public:
    CountMinSketch(int w, int d) : width(w), depth(d), table(d, std::vector<int>(w, 0)) {}

    void add(int value) {
        for (int i = 0; i < depth; ++i) {
            int index = universalHash(value, i);
            table[i][index]++;
        }
    }

    void remove(int value) {
        for (int i = 0; i < depth; ++i) {
            int index = universalHash(value, i);
            table[i][index] = std::max(0, table[i][index] - 1); // Avoid underflow
        }
    }

    int estimate(int value) const {
        int min_count = INT_MAX;
        for (int i = 0; i < depth; ++i) {
            int index = universalHash(value, i);
            min_count = std::min(min_count, table[i][index]);
        }
        return min_count;
    }

    // **Getter methods for width and depth**
    int getWidth() const { return width; }
    int getDepth() const { return depth; }
};

// **Improved IncrementalSampler Class**
class IncrementalSampler {
private:
    double sampling_rate;
    std::default_random_engine generator;
    std::uniform_real_distribution<double> distribution;

public:
    IncrementalSampler(double rate)
        : sampling_rate(rate), distribution(0.0, 1.0), generator(std::random_device{}()) {}

    bool shouldSample() {
        return distribution(generator) < sampling_rate;
    }
};

// **Improved CEEngine Class**
class CEEngine {
public:
    CEEngine(int num, DataExecuter *dataExecuter);
    ~CEEngine() = default;

    void insertTuple(const std::vector<int>& tuple);
    void deleteTuple(const std::vector<int>& tuple, int tupleId);
    int query(const std::vector<CompareExpression>& quals);
    void prepare();

private:
    CountMinSketch countMinA;
    CountMinSketch countMinB;
    IncrementalSampler sampler;
    DataExecuter *dataExecuter;
};

// **Constructor Implementation**
CEEngine::CEEngine(int num, DataExecuter *dataExecuter)
    : countMinA(std::ceil(num * 0.01), 5), // CMS width based on 1% of dataset size
      countMinB(std::ceil(num * 0.01), 5),
      sampler(0.1) {
    this->dataExecuter = dataExecuter;
    prepare();
}

// **insertTuple Method**
void CEEngine::insertTuple(const std::vector<int>& tuple) {
    if (tuple.size() < 2) {
        throw std::invalid_argument("Tuple must contain at least two elements.");
    }

    int valueA = tuple[0];
    int valueB = tuple[1];

    if (sampler.shouldSample()) {
        countMinA.add(valueA);
        countMinB.add(valueB);
    }
}

// **deleteTuple Method**
void CEEngine::deleteTuple(const std::vector<int>& tuple, int tupleId) {
    if (tuple.size() < 2) {
        throw std::invalid_argument("Tuple must contain at least two elements.");
    }

    int valueA = tuple[0];
    int valueB = tuple[1];

    countMinA.remove(valueA);
    countMinB.remove(valueB);
}

// **query Method**
int CEEngine::query(const std::vector<CompareExpression>& quals) {
    if (quals.empty()) {
        throw std::invalid_argument("Query conditions cannot be empty.");
    }

    std::unordered_map<int, int> columnEstimates;

    for (const auto& qual : quals) {
        int estimate = (qual.columnIdx == 0)
                           ? countMinA.estimate(qual.value)
                           : countMinB.estimate(qual.value);

        if (columnEstimates.find(qual.columnIdx) == columnEstimates.end()) {
            columnEstimates[qual.columnIdx] = estimate;
        } else {
            columnEstimates[qual.columnIdx] = std::min(columnEstimates[qual.columnIdx], estimate);
        }
    }

    int finalResult = INT_MAX;
    for (const auto& pair : columnEstimates) {
        finalResult = std::min(finalResult, pair.second);
    }

    return finalResult;
}

// **prepare Method**
void CEEngine::prepare() {
    // Reset the CMS tables
    countMinA = CountMinSketch(countMinA.getWidth(), countMinA.getDepth());
    countMinB = CountMinSketch(countMinB.getWidth(), countMinB.getDepth());
}

#endif
