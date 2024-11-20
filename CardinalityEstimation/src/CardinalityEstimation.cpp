#include <common/Root.h>
#include <CardinalityEstimation.h>

CEEngine::CEEngine(int num, DataExecuter *dataExecuter)
    : countMinA(100, 5), countMinB(100, 5), sampler(0.1) {
    this->dataExecuter = dataExecuter;
    prepare();  // Prepare CMS and sampling
}

void CEEngine::insertTuple(const std::vector<int>& tuple) {
    int valueA = tuple[0];
    int valueB = tuple[1];

    if (sampler.shouldSample()) {
        countMinA.add(valueA);
        countMinB.add(valueB);
    }
}

void CEEngine::deleteTuple(const std::vector<int>& tuple, int tupleId) {
    int valueA = tuple[0];
    int valueB = tuple[1];

    countMinA.remove(valueA);
    countMinB.remove(valueB);
}

int CEEngine::query(const std::vector<CompareExpression>& quals) {
    int columnAResult = INT_MAX;
    int columnBResult = INT_MAX;

    for (const auto& qual : quals) {
        if (qual.columnIdx == 0) {
            columnAResult = countMinA.estimate(qual.value);
        }
        if (qual.columnIdx == 1) {
            columnBResult = countMinB.estimate(qual.value);
        }
    }

    return std::min(columnAResult, columnBResult);
}

void CEEngine::prepare() {
    // Reset or initialize any components if necessary
}
