
#include <iostream>
#include <ctime>

int my_strcmp(const char* s1, const char* s2) {
    int i = 0;
    while (s1[i] != '\0' && s2[i] != '\0') {
        if (s1[i] != s2[i]) {
            return s1[i] - s2[i];
        }
        i++;
    }
    return s1[i] - s2[i];
}

class DynamicArray {
private:
    int* data;
    int capacity;
    int size;
    
    void resize() {
        capacity *= 2;
        int* newData = new int[capacity];
        for (int i = 0; i < size; i++) {
            newData[i] = data[i];
        }
        delete[] data;
        data = newData;
    }
    
public:
    DynamicArray() : capacity(10), size(0) {
        data = new int[capacity];
    }
    
    DynamicArray(const DynamicArray& other) : capacity(other.capacity), size(other.size) {
        data = new int[capacity];
        for (int i = 0; i < size; i++) {
            data[i] = other.data[i];
        }
    }
    
    ~DynamicArray() {
        delete[] data;
    }
    
    void add(int value) {
        if (size >= capacity) {
            resize();
        }
        data[size++] = value;
    }
    
    int get(int index) const {
        return (index >= 0 && index < size) ? data[index] : -1;
    }
    
    int getSize() const { return size; }
    
    bool contains(int value) const {
        for (int i = 0; i < size; i++) {
            if (data[i] == value) return true;
        }
        return false;
    }
    
    void clear() {
        size = 0;
    }
    
    DynamicArray& operator=(const DynamicArray& other) {
        if (this != &other) {
            delete[] data;
            capacity = other.capacity;
            size = other.size;
            data = new int[capacity];
            for (int i = 0; i < size; i++) {
                data[i] = other.data[i];
            }
        }
        return *this;
    }
};

struct DocumentInfo {
    int docId;
    char url[512];
    int termCount;
};

struct TermInfo {
    char term[256];
    DynamicArray docIds;
};

class IndexReader {
private:
    FILE* file;
    int numTerms;
    int numDocs;
    long long invertedIndexOffset;
    long long forwardIndexOffset;
    
    
    TermInfo* termCache;
    int termCacheSize;
    
    
    DocumentInfo* docCache;
    int docCacheSize;
    
    unsigned int readUInt32() {
        unsigned char bytes[4];
        fread(bytes, 1, 4, file);
        return bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24);
    }
    
    unsigned long long readUInt64() {
        unsigned char bytes[8];
        fread(bytes, 1, 8, file);
        unsigned long long result = 0;
        for (int i = 0; i < 8; i++) {
            result |= ((unsigned long long)bytes[i]) << (i * 8);
        }
        return result;
    }
    
    unsigned short readUInt16() {
        unsigned char bytes[2];
        fread(bytes, 1, 2, file);
        return bytes[0] | (bytes[1] << 8);
    }
    
    void loadAllTerms() {
        fseek(file, invertedIndexOffset, SEEK_SET);
        
        termCache = new TermInfo[numTerms];
        termCacheSize = 0;
        
        std::cout << "Загрузка " << numTerms << " термов в память..." << std::endl;
        
        for (int i = 0; i < numTerms; i++) {
            unsigned short termLen = readUInt16();
            fread(termCache[i].term, 1, termLen, file);
            termCache[i].term[termLen] = '\0';
            
            unsigned int docCount = readUInt32();
            for (unsigned int j = 0; j < docCount; j++) {
                unsigned int docId = readUInt32();
                termCache[i].docIds.add(docId);
            }
            
            termCacheSize++;
            
            if ((i + 1) % 10000 == 0) {
                std::cout << "  Загружено: " << (i + 1) << std::endl;
            }
        }
        
        std::cout << "Термы загружены в память." << std::endl;
    }
    
    void loadAllDocuments() {
        fseek(file, forwardIndexOffset, SEEK_SET);
        
        docCache = new DocumentInfo[numDocs];
        docCacheSize = 0;
        
        std::cout << "Загрузка " << numDocs << " документов в память..." << std::endl;
        
        for (int i = 0; i < numDocs; i++) {
            docCache[i].docId = readUInt32();
            
            unsigned short urlLen = readUInt16();
            fread(docCache[i].url, 1, urlLen, file);
            docCache[i].url[urlLen] = '\0';
            
            docCache[i].termCount = readUInt32();
            
            docCacheSize++;
        }
        
        std::cout << "Документы загружены в память." << std::endl;
    }
    
    bool streq(const char* s1, const char* s2) const {
        int i = 0;
        while (s1[i] != '\0' && s2[i] != '\0') {
            if (s1[i] != s2[i]) return false;
            i++;
        }
        return s1[i] == s2[i];
    }
    
    
    int binarySearchTerm(const char* term) const {
        int left = 0;
        int right = termCacheSize - 1;
        
        while (left <= right) {
            int mid = (left + right) / 2;
            
            int cmp = 0;
            int i = 0;
            while (term[i] != '\0' && termCache[mid].term[i] != '\0') {
                if (term[i] < termCache[mid].term[i]) {
                    cmp = -1;
                    break;
                } else if (term[i] > termCache[mid].term[i]) {
                    cmp = 1;
                    break;
                }
                i++;
            }
            if (cmp == 0) {
                if (term[i] == '\0' && termCache[mid].term[i] != '\0') cmp = -1;
                else if (term[i] != '\0' && termCache[mid].term[i] == '\0') cmp = 1;
            }
            
            if (cmp == 0) {
                return mid;
            } else if (cmp < 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        
        return -1;
    }
    
public:
    IndexReader() : file(nullptr), termCache(nullptr), docCache(nullptr),
                    termCacheSize(0), docCacheSize(0) {}
    
    ~IndexReader() {
        if (file) fclose(file);
        if (termCache) delete[] termCache;
        if (docCache) delete[] docCache;
    }
    
    bool loadIndex(const char* filename) {
        file = fopen(filename, "rb");
        if (!file) {
            std::cerr << "Ошибка открытия индекса: " << filename << std::endl;
            return false;
        }
        
        std::cout << "Загрузка индекса из " << filename << "..." << std::endl;
        
        
        char magic[5] = {0};
        fread(magic, 1, 4, file);
        
        if (!streq(magic, "SIDX")) {
            std::cerr << "Неверный формат индекса!" << std::endl;
            return false;
        }
        
        unsigned int version = readUInt32();
        numTerms = readUInt32();
        numDocs = readUInt32();
        invertedIndexOffset = readUInt64();
        forwardIndexOffset = readUInt64();
        
        std::cout << "  Версия: " << version << std::endl;
        std::cout << "  Термов: " << numTerms << std::endl;
        std::cout << "  Документов: " << numDocs << std::endl;
        
        
        loadAllTerms();
        loadAllDocuments();
        
        return true;
    }
    
    const DynamicArray* searchTerm(const char* term) const {
        int idx = binarySearchTerm(term);
        if (idx == -1) {
            return nullptr;
        }
        return &termCache[idx].docIds;
    }
    
    const DocumentInfo* getDocument(int docId) const {
        
        for (int i = 0; i < docCacheSize; i++) {
            if (docCache[i].docId == docId) {
                return &docCache[i];
            }
        }
        return nullptr;
    }
    
    int getNumDocs() const { return numDocs; }
};

class SimpleStemmer {
private:
    char buffer[256];
    int len;
    
    void copy(const char* str) {
        len = 0;
        while (str[len] != '\0' && len < 255) {
            char c = str[len];
            if (c >= 'A' && c <= 'Z') c = c + 32;
            buffer[len] = c;
            len++;
        }
        buffer[len] = '\0';
    }
    
    bool removeIfEndsWith(const char* ending) {
        int endLen = 0;
        while (ending[endLen] != '\0') endLen++;
        
        if (len < endLen || len - endLen < 3) return false;
        
        for (int i = 0; i < endLen; i++) {
            if (buffer[len - endLen + i] != ending[i]) {
                return false;
            }
        }
        
        len -= endLen;
        buffer[len] = '\0';
        return true;
    }
    
public:
    const char* stem(const char* word) {
        copy(word);
        if (len < 4) return buffer;
        
        
        if (removeIfEndsWith("\xD0\xB0\xD0\xBC\xD0\xB8")) return buffer;
        if (removeIfEndsWith("\xD1\x8F\xD0\xBC\xD0\xB8")) return buffer;
        if (removeIfEndsWith("\xD0\xBE\xD0\xB2")) return buffer;
        if (removeIfEndsWith("\xD0\xB5\xD0\xB2")) return buffer;
        if (removeIfEndsWith("\xD0\xB0\xD1\x85")) return buffer;
        if (removeIfEndsWith("\xD1\x8F\xD1\x85")) return buffer;
        if (removeIfEndsWith("\xD0\xBE\xD0\xBC")) return buffer;
        if (removeIfEndsWith("\xD0\xB5\xD0\xBC")) return buffer;
        if (removeIfEndsWith("\xD0\xBE\xD0\xB9")) return buffer;
        if (removeIfEndsWith("\xD0\xB5\xD0\xB9")) return buffer;
        if (removeIfEndsWith("\xD1\x8B\xD0\xB9")) return buffer;
        if (removeIfEndsWith("\xD0\xB8\xD0\xB9")) return buffer;
        if (removeIfEndsWith("\xD0\xB0\xD1\x8F")) return buffer;
        if (removeIfEndsWith("\xD0\xBE\xD0\xB5")) return buffer;
        if (removeIfEndsWith("\xD0\xB5\xD0\xB5")) return buffer;
        if (removeIfEndsWith("\xD1\x8B\xD0\xB5")) return buffer;
        if (removeIfEndsWith("\xD0\xB8\xD0\xB5")) return buffer;
        if (removeIfEndsWith("\xD1\x82\xD1\x8C")) return buffer;
        if (removeIfEndsWith("\xD0\xB5\xD1\x82")) return buffer;
        if (removeIfEndsWith("\xD0\xB8\xD1\x82")) return buffer;
        if (removeIfEndsWith("\xD1\x83")) return buffer;
        if (removeIfEndsWith("\xD0\xB0")) return buffer;
        if (removeIfEndsWith("\xD1\x8F")) return buffer;
        if (removeIfEndsWith("\xD1\x8B")) return buffer;
        if (removeIfEndsWith("\xD0\xB8")) return buffer;
        if (removeIfEndsWith("\xD0\xBE")) return buffer;
        if (removeIfEndsWith("\xD0\xB5")) return buffer;
        
        
        if (removeIfEndsWith("ing")) return buffer;
        if (removeIfEndsWith("ed")) return buffer;
        if (removeIfEndsWith("s")) return buffer;
        
        return buffer;
    }
};

class BooleanOperations {
public:
    
    static DynamicArray intersect(const DynamicArray& list1, const DynamicArray& list2) {
        DynamicArray result;
        int i = 0, j = 0;
        
        while (i < list1.getSize() && j < list2.getSize()) {
            int id1 = list1.get(i);
            int id2 = list2.get(j);
            
            if (id1 == id2) {
                result.add(id1);
                i++;
                j++;
            } else if (id1 < id2) {
                i++;
            } else {
                j++;
            }
        }
        
        return result;
    }
    
    
    static DynamicArray unionLists(const DynamicArray& list1, const DynamicArray& list2) {
        DynamicArray result;
        int i = 0, j = 0;
        
        while (i < list1.getSize() && j < list2.getSize()) {
            int id1 = list1.get(i);
            int id2 = list2.get(j);
            
            if (id1 == id2) {
                result.add(id1);
                i++;
                j++;
            } else if (id1 < id2) {
                result.add(id1);
                i++;
            } else {
                result.add(id2);
                j++;
            }
        }
        
        while (i < list1.getSize()) {
            result.add(list1.get(i++));
        }
        
        while (j < list2.getSize()) {
            result.add(list2.get(j++));
        }
        
        return result;
    }
    
    
    static DynamicArray negate(const DynamicArray& list, int totalDocs) {
        DynamicArray result;
        int j = 0;
        
        for (int docId = 1; docId <= totalDocs; docId++) {
            if (j < list.getSize() && list.get(j) == docId) {
                j++;
            } else {
                result.add(docId);
            }
        }
        
        return result;
    }
};

enum TokenType {
    TOKEN_WORD,
    TOKEN_AND,
    TOKEN_OR,
    TOKEN_NOT,
    TOKEN_LPAREN,
    TOKEN_RPAREN,
    TOKEN_END
};

struct Token {
    TokenType type;
    char value[256];
};

class QueryParser {
private:
    const char* input;
    int pos;
    Token currentToken;
    
    IndexReader* index;
    SimpleStemmer stemmer;
    int totalDocs;
    
    void skipWhitespace() {
        while (input[pos] == ' ' || input[pos] == '\t' || input[pos] == '\n') {
            pos++;
        }
    }
    
    bool isLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || 
               (c >= '0' && c <= '9') || (unsigned char)c >= 0x80;
    }
    
    void nextToken() {
        skipWhitespace();
        
        if (input[pos] == '\0') {
            currentToken.type = TOKEN_END;
            return;
        }
        
        
        if (input[pos] == '(') {
            currentToken.type = TOKEN_LPAREN;
            pos++;
            return;
        }
        
        if (input[pos] == ')') {
            currentToken.type = TOKEN_RPAREN;
            pos++;
            return;
        }
        
        
        if (input[pos] == '!') {
            currentToken.type = TOKEN_NOT;
            pos++;
            return;
        }
        
        
        if (input[pos] == '|' && input[pos+1] == '|') {
            currentToken.type = TOKEN_OR;
            pos += 2;
            return;
        }
        
        
        if (input[pos] == '&' && input[pos+1] == '&') {
            currentToken.type = TOKEN_AND;
            pos += 2;
            return;
        }
        
        
        if (isLetter(input[pos])) {
            int i = 0;
            while (isLetter(input[pos]) && i < 255) {
                currentToken.value[i++] = input[pos++];
            }
            currentToken.value[i] = '\0';
            currentToken.type = TOKEN_WORD;
            return;
        }
        
        
        pos++;
        nextToken();
    }
    
    DynamicArray parseExpression();
    DynamicArray parseTerm();
    DynamicArray parseFactor();
    
public:
    QueryParser(IndexReader* idx, int numDocs) : index(idx), totalDocs(numDocs), pos(0) {}
    
    DynamicArray parse(const char* query) {
        input = query;
        pos = 0;
        nextToken();
        return parseExpression();
    }
};


DynamicArray QueryParser::parseExpression() {
    DynamicArray result = parseTerm();
    
    while (currentToken.type == TOKEN_OR) {
        nextToken();
        DynamicArray right = parseTerm();
        result = BooleanOperations::unionLists(result, right);
    }
    
    return result;
}


DynamicArray QueryParser::parseTerm() {
    DynamicArray result = parseFactor();
    
    while (currentToken.type == TOKEN_AND || 
           currentToken.type == TOKEN_WORD || 
           currentToken.type == TOKEN_NOT ||
           currentToken.type == TOKEN_LPAREN) {
        
        if (currentToken.type == TOKEN_AND) {
            nextToken();
        }
        
        DynamicArray right = parseFactor();
        result = BooleanOperations::intersect(result, right);
    }
    
    return result;
}


DynamicArray QueryParser::parseFactor() {
    if (currentToken.type == TOKEN_NOT) {
        nextToken();
        DynamicArray operand = parseFactor();
        return BooleanOperations::negate(operand, totalDocs);
    }
    
    if (currentToken.type == TOKEN_LPAREN) {
        nextToken();
        DynamicArray result = parseExpression();
        if (currentToken.type == TOKEN_RPAREN) {
            nextToken();
        }
        return result;
    }
    
    if (currentToken.type == TOKEN_WORD) {
        
        const char* stem = stemmer.stem(currentToken.value);
        
        
        const DynamicArray* postings = index->searchTerm(stem);
        
        nextToken();
        
        if (postings) {
            return *postings;
        } else {
            
            return DynamicArray();
        }
    }
    
    
    return DynamicArray();
}

void printResults(const DynamicArray& results, IndexReader& index, int maxResults = 50) {
    std::cout << "\nНайдено документов: " << results.getSize() << std::endl;
    
    if (results.getSize() == 0) {
        std::cout << "Ничего не найдено." << std::endl;
        return;
    }
    
    std::cout << "\nРезультаты (первые " << maxResults << "):" << std::endl;
    std::cout << std::string(80, '-') << std::endl;
    
    for (int i = 0; i < results.getSize() && i < maxResults; i++) {
        int docId = results.get(i);
        const DocumentInfo* doc = index.getDocument(docId);
        
        if (doc) {
            std::cout << (i + 1) << ". [Doc " << docId << "] " << doc->url << std::endl;
        }
    }
    
    if (results.getSize() > maxResults) {
        std::cout << "\n... и ещё " << (results.getSize() - maxResults) << " результатов" << std::endl;
    }
}

void interactiveSearch(IndexReader& index) {
    std::cout << "\n=== ИНТЕРАКТИВНЫЙ ПОИСК ===" << std::endl;
    std::cout << "Синтаксис:" << std::endl;
    std::cout << "  пробел или && - AND" << std::endl;
    std::cout << "  || - OR" << std::endl;
    std::cout << "  ! - NOT" << std::endl;
    std::cout << "  () - группировка" << std::endl;
    std::cout << "Введите 'exit' для выхода\n" << std::endl;
    
    QueryParser parser(&index, index.getNumDocs());
    char query[1024];
    
    while (true) {
        std::cout << "\nЗапрос> ";
        std::cin.getline(query, 1024);
        
        if (my_strcmp(query, "exit") == 0) {
            break;
        }
        
        if (query[0] == '\0') {
            continue;
        }
        
        clock_t start = clock();
        DynamicArray results = parser.parse(query);
        clock_t end = clock();
        
        double time = (double)(end - start) / CLOCKS_PER_SEC * 1000;
        
        printResults(results, index);
        std::cout << "\nВремя поиска: " << time << " мс" << std::endl;
    }
}

void batchSearch(IndexReader& index, const char* inputFile, const char* outputFile) {
    FILE* fin = fopen(inputFile, "r");
    if (!fin) {
        std::cerr << "Ошибка открытия файла: " << inputFile << std::endl;
        return;
    }
    
    FILE* fout = fopen(outputFile, "w");
    if (!fout) {
        std::cerr << "Ошибка создания файла: " << outputFile << std::endl;
        fclose(fin);
        return;
    }
    
    QueryParser parser(&index, index.getNumDocs());
    char query[1024];
    int queryNum = 0;
    
    std::cout << "\n=== ПАКЕТНЫЙ ПОИСК ===" << std::endl;
    
    while (fgets(query, 1024, fin)) {
        
        int len = 0;
        while (query[len] != '\0' && query[len] != '\n' && query[len] != '\r') len++;
        query[len] = '\0';
        
        if (query[0] == '\0') continue;
        
        queryNum++;
        std::cout << "Запрос #" << queryNum << ": " << query << std::endl;
        
        clock_t start = clock();
        DynamicArray results = parser.parse(query);
        clock_t end = clock();
        
        double time = (double)(end - start) / CLOCKS_PER_SEC * 1000;
        
        fprintf(fout, "Query #%d: %s\n", queryNum, query);
        fprintf(fout, "Found: %d documents\n", results.getSize());
        fprintf(fout, "Time: %.3f ms\n", time);
        fprintf(fout, "Results: ");
        
        for (int i = 0; i < results.getSize() && i < 100; i++) {
            fprintf(fout, "%d ", results.get(i));
        }
        fprintf(fout, "\n\n");
        
        std::cout << "  Найдено: " << results.getSize() << " документов за " << time << " мс" << std::endl;
    }
    
    fclose(fin);
    fclose(fout);
    
    std::cout << "\nРезультаты сохранены в " << outputFile << std::endl;
}

int main(int argc, char* argv[]) {
    std::cout << "=== БУЛЕВ ПОИСК ===" << std::endl;
    std::cout << std::endl;
    
    
    IndexReader index;
    if (!index.loadIndex("../lab6/index.bin")) {
        return 1;
    }
    
    std::cout << "\nИндекс загружен!" << std::endl;
    
    
    if (argc == 1) {
        
        interactiveSearch(index);
    } else if (argc == 3) {
        
        batchSearch(index, argv[1], argv[2]);
    } else {
        std::cout << "Использование:" << std::endl;
        std::cout << "  " << argv[0] << "                    - интерактивный режим" << std::endl;
        std::cout << "  " << argv[0] << " <input> <output>  - пакетный режим" << std::endl;
    }
    
    return 0;
}
