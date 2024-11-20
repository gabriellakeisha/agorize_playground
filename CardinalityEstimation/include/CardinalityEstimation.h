#ifndef CARDINALITYESTIMATION_ENGINE
#define CARDINALITYESTIMATION_ENGINE

// Including necessary libraries
#include <vector>  // To use vectors for storing data
#include <unordered_map>  // To create hash maps
#include <cmath>  // For mathematical operations
#include <algorithm>  // To use min function
#include <random>  // To implement sampling mechanism
#include <executer/DataExecuter.h>  // Custom library for managing data execution
#include <common/Expression.h>  // Custom library for expressions used in queries

// **CountMinSketch Class**:
// This class implements a Count-Min Sketch (CMS), a probabilistic data structure that estimates the frequency of elements in a memory-efficient way.
class CountMinSketch {
private:
    std::vector<std::vector<int>> table;  // 2D vector representing the CMS table
    int width;  // Number of columns (hash buckets)
    int depth;  // Number of rows (different hash functions)
    std::hash<int> hash_fn;  // Hash function to create indices

public:
    // Constructor that initializes the CMS with given width and depth
    CountMinSketch(int w, int d) : width(w), depth(d), table(d, std::vector<int>(w, 0)) {}

    // Add a value to the CMS by incrementing corresponding hash bucket counts
    void add(int value) {
        for (int i = 0; i < depth; ++i) {
            int index = (hash_fn(value) + i) % width;
            table[i][index]++;
        }
    }

    // Remove a value from the CMS by decrementing corresponding hash bucket counts
    void remove(int value) {
        for (int i = 0; i < depth; ++i) {
            int index = (hash_fn(value) + i) % width;
            table[i][index]--;
        }
    }

    // Estimate the frequency of a given value by taking the minimum count across all hash functions
    int estimate(int value) const {
        int min_count = INT_MAX;
        for (int i = 0; i < depth; ++i) {
            int index = (hash_fn(value) + i) % width;
            min_count = std::min(min_count, table[i][index]);
        }
        return min_count;
    }
};

// **IncrementalSampler Class**:
// This class helps to dynamically decide whether an element should be sampled based on a given probability.
class IncrementalSampler {
private:
    double sampling_rate;
    std::default_random_engine generator;  // Random number generator
    std::uniform_real_distribution<double> distribution;  // Distribution to decide sampling

public:
    // Constructor initializing the sampling rate and random distribution
    IncrementalSampler(double rate) : sampling_rate(rate), distribution(0.0, 1.0) {}

    // Function to determine if a value should be sampled
    bool shouldSample() {
        return distribution(generator) < sampling_rate;
    }
};

// **CEEngine Class**:
// This class handles cardinality estimation tasks using the Count-Min Sketch and Incremental Sampler.
class CEEngine {
public:
    // Constructor initializing the CMS and sampler
    CEEngine(int num, DataExecuter *dataExecuter);
    ~CEEngine() = default;

    // Method to insert a new tuple into the dataset
    void insertTuple(const std::vector<int>& tuple);

    // Method to delete a tuple from the dataset
    void deleteTuple(const std::vector<int>& tuple, int tupleId);

    // Query the dataset for estimated cardinality based on conditions
    int query(const std::vector<CompareExpression>& quals);

    // Prepare the CMS and sampler for use before any operation
    void prepare();

private:
    CountMinSketch countMinA;  // CMS for column A
    CountMinSketch countMinB;  // CMS for column B
    IncrementalSampler sampler;  // Sampler for managing data skewness
    DataExecuter *dataExecuter;  // Data executer to manage data interactions
};

// **Constructor Implementation**:
// Initializes the Count-Min Sketch for two columns, A and B, and sets up the sampler.
CEEngine::CEEngine(int num, DataExecuter *dataExecuter)
    : countMinA(100, 5), countMinB(100, 5), sampler(0.1) {
    this->dataExecuter = dataExecuter;
    prepare();  // Prepare the data structures for use
}

// **insertTuple Method**:
// Inserts a tuple into the CMS and decides whether to sample the tuple using the sampler.
void CEEngine::insertTuple(const std::vector<int>& tuple) {
    int valueA = tuple[0];  // Extract value for column A
    int valueB = tuple[1];  // Extract value for column B

    if (sampler.shouldSample()) {
        countMinA.add(valueA);  // Add to CMS for column A if sampled
        countMinB.add(valueB);  // Add to CMS for column B if sampled
    }
}

// **deleteTuple Method**:
// Deletes a tuple from the CMS, though Bloom Filters are not used for deletions in this implementation.
void CEEngine::deleteTuple(const std::vector<int>& tuple, int tupleId) {
    int valueA = tuple[0];  // Extract value for column A
    int valueB = tuple[1];  // Extract value for column B

    countMinA.remove(valueA);  // Remove from CMS for column A
    countMinB.remove(valueB);  // Remove from CMS for column B
}

// **query Method**:
// Estimates the cardinality of tuples based on a given query using the CMS.
int CEEngine::query(const std::vector<CompareExpression>& quals) {
    int columnAResult = INT_MAX;
    int columnBResult = INT_MAX;

    for (const auto& qual : quals) {
        if (qual.columnIdx == 0) {  // Column A
            columnAResult = countMinA.estimate(qual.value);  // Estimate frequency for column A
        }
        if (qual.columnIdx == 1) {  // Column B
            columnBResult = countMinB.estimate(qual.value);  // Estimate frequency for column B
        }
    }

    return std::min(columnAResult, columnBResult);  // Return the minimum estimate across columns
}

// **prepare Method**:
// Prepares the CMS for use, allowing data structures to be reset if necessary.
void CEEngine::prepare() {
    // Any initialization or reset of the data structures can be done here if needed
}

#endif