#include <iostream>
#include <ctime>

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
    
    int* getData() { return data; }
    
    
    bool contains(int value) const {
        for (int i = 0; i < size; i++) {
            if (data[i] == value) return true;
        }
        return false;
    }
    
    
    void sort() {
        
        quickSort(0, size - 1);
    }
    
private:
    void quickSort(int low, int high) {
        if (low < high) {
            int pivot = data[high];
            int i = low - 1;
            
            for (int j = low; j < high; j++) {
                if (data[j] < pivot) {
                    i++;
                    int temp = data[i];
                    data[i] = data[j];
                    data[j] = temp;
                }
            }
            
            int temp = data[i + 1];
            data[i + 1] = data[high];
            data[high] = temp;
            
            int pi = i + 1;
            quickSort(low, pi - 1);
            quickSort(pi + 1, high);
        }
    }
};

class StringArray {
private:
    char** data;
    int capacity;
    int size;
    
    void resize() {
        capacity *= 2;
        char** newData = new char*[capacity];
        for (int i = 0; i < size; i++) {
            newData[i] = data[i];
        }
        delete[] data;
        data = newData;
    }
    
public:
    StringArray() : capacity(100), size(0) {
        data = new char*[capacity];
    }
    
    ~StringArray() {
        for (int i = 0; i < size; i++) {
            delete[] data[i];
        }
        delete[] data;
    }
    
    void add(const char* str) {
        if (size >= capacity) {
            resize();
        }
        
        int len = 0;
        while (str[len] != '\0') len++;
        
        data[size] = new char[len + 1];
        for (int i = 0; i <= len; i++) {
            data[size][i] = str[i];
        }
        size++;
    }
    
    int getSize() const { return size; }
    
    const char* get(int index) const {
        if (index >= 0 && index < size) {
            return data[index];
        }
        return nullptr;
    }
    
    char** getData() { return data; }
};

struct PostingList {
    DynamicArray docIds;
    
    void addDocument(int docId) {
        if (!docIds.contains(docId)) {
            docIds.add(docId);
        }
    }
    
    void finalize() {
        docIds.sort();
    }
};

struct TermEntry {
    char* term;
    PostingList postings;
    TermEntry* next;
    
    TermEntry(const char* t) : next(nullptr) {
        int len = 0;
        while (t[len] != '\0') len++;
        term = new char[len + 1];
        for (int i = 0; i <= len; i++) {
            term[i] = t[i];
        }
    }
    
    ~TermEntry() {
        delete[] term;
    }
};

class InvertedIndex {
private:
    static const int TABLE_SIZE = 20011; 
    TermEntry** table;
    int uniqueTerms;
    long long totalTermOccurrences;
    
    unsigned long hash(const char* str) const {
        unsigned long hash = 5381;
        int c;
        while ((c = *str++)) {
            hash = ((hash << 5) + hash) + c;
        }
        return hash % TABLE_SIZE;
    }
    
    bool streq(const char* s1, const char* s2) const {
        int i = 0;
        while (s1[i] != '\0' && s2[i] != '\0') {
            if (s1[i] != s2[i]) return false;
            i++;
        }
        return s1[i] == s2[i];
    }
    
public:
    InvertedIndex() : uniqueTerms(0), totalTermOccurrences(0) {
        table = new TermEntry*[TABLE_SIZE];
        for (int i = 0; i < TABLE_SIZE; i++) {
            table[i] = nullptr;
        }
    }
    
    ~InvertedIndex() {
        for (int i = 0; i < TABLE_SIZE; i++) {
            TermEntry* entry = table[i];
            while (entry) {
                TermEntry* next = entry->next;
                delete entry;
                entry = next;
            }
        }
        delete[] table;
    }
    
    void addTerm(const char* term, int docId) {
        unsigned long idx = hash(term);
        TermEntry* entry = table[idx];
        
        
        while (entry) {
            if (streq(entry->term, term)) {
                entry->postings.addDocument(docId);
                totalTermOccurrences++;
                return;
            }
            entry = entry->next;
        }
        
        
        TermEntry* newEntry = new TermEntry(term);
        newEntry->postings.addDocument(docId);
        newEntry->next = table[idx];
        table[idx] = newEntry;
        uniqueTerms++;
        totalTermOccurrences++;
    }
    
    int getUniqueTerms() const { return uniqueTerms; }
    long long getTotalOccurrences() const { return totalTermOccurrences; }
    
    void getAllTerms(TermEntry** result, int& count) const {
        count = 0;
        for (int i = 0; i < TABLE_SIZE; i++) {
            TermEntry* entry = table[i];
            while (entry) {
                result[count++] = entry;
                entry = entry->next;
            }
        }
    }
    
    void finalizeAllPostings() {
        for (int i = 0; i < TABLE_SIZE; i++) {
            TermEntry* entry = table[i];
            while (entry) {
                entry->postings.finalize();
                entry = entry->next;
            }
        }
    }
};

struct DocumentMetadata {
    int docId;
    char url[512];
    int termCount;
};

class ForwardIndex {
private:
    DocumentMetadata* documents;
    int capacity;
    int size;
    
    void resize() {
        capacity *= 2;
        DocumentMetadata* newDocs = new DocumentMetadata[capacity];
        for (int i = 0; i < size; i++) {
            newDocs[i] = documents[i];
        }
        delete[] documents;
        documents = newDocs;
    }
    
public:
    ForwardIndex() : capacity(1000), size(0) {
        documents = new DocumentMetadata[capacity];
    }
    
    ~ForwardIndex() {
        delete[] documents;
    }
    
    void addDocument(int docId, const char* url, int termCount) {
        if (size >= capacity) {
            resize();
        }
        
        documents[size].docId = docId;
        documents[size].termCount = termCount;
        
        int i = 0;
        while (url[i] != '\0' && i < 511) {
            documents[size].url[i] = url[i];
            i++;
        }
        documents[size].url[i] = '\0';
        
        size++;
    }
    
    int getSize() const { return size; }
    
    const DocumentMetadata* getDocument(int index) const {
        if (index >= 0 && index < size) {
            return &documents[index];
        }
        return nullptr;
    }
    
    DocumentMetadata* getAllDocuments() { return documents; }
};

void quickSortTerms(TermEntry** arr, int low, int high) {
    if (low < high) {
        TermEntry* pivot = arr[high];
        int i = low - 1;
        
        for (int j = low; j < high; j++) {
            
            int cmp = 0;
            int k = 0;
            while (arr[j]->term[k] != '\0' && pivot->term[k] != '\0') {
                if (arr[j]->term[k] < pivot->term[k]) {
                    cmp = -1;
                    break;
                } else if (arr[j]->term[k] > pivot->term[k]) {
                    cmp = 1;
                    break;
                }
                k++;
            }
            if (cmp == 0) {
                if (arr[j]->term[k] == '\0' && pivot->term[k] != '\0') cmp = -1;
                else if (arr[j]->term[k] != '\0' && pivot->term[k] == '\0') cmp = 1;
            }
            
            if (cmp < 0) {
                i++;
                TermEntry* temp = arr[i];
                arr[i] = arr[j];
                arr[j] = temp;
            }
        }
        
        TermEntry* temp = arr[i + 1];
        arr[i + 1] = arr[high];
        arr[high] = temp;
        
        int pi = i + 1;
        quickSortTerms(arr, low, pi - 1);
        quickSortTerms(arr, pi + 1, high);
    }
}

class CSVParser {
private:
    FILE* file;
    char line[2048];
    
public:
    CSVParser() : file(nullptr) {}
    
    ~CSVParser() {
        if (file) fclose(file);
    }
    
    bool open(const char* filename) {
        file = fopen(filename, "r");
        if (!file) {
            std::cerr << "Ошибка открытия файла: " << filename << std::endl;
            return false;
        }
        
        
        fgets(line, sizeof(line), file);
        
        return true;
    }
    
    bool readNext(int& docId, char* token) {
        if (!file || feof(file)) return false;
        
        if (!fgets(line, sizeof(line), file)) {
            return false;
        }
        
        
        int i = 0;
        
        
        char docIdStr[20];
        int j = 0;
        while (line[i] != ',' && line[i] != '\0' && j < 19) {
            docIdStr[j++] = line[i++];
        }
        docIdStr[j] = '\0';
        docId = 0;
        for (int k = 0; docIdStr[k] != '\0'; k++) {
            docId = docId * 10 + (docIdStr[k] - '0');
        }
        
        if (line[i] == ',') i++;
        
        
        j = 0;
        while (line[i] != '\0' && line[i] != '\n' && line[i] != '\r' && j < 255) {
            token[j++] = line[i++];
        }
        token[j] = '\0';
        
        return true;
    }
};

class SimpleXMLParser {
private:
    char* content;
    long contentSize;
    
    bool startsWith(const char* str, const char* prefix, long pos) const {
        int i = 0;
        while (prefix[i] != '\0') {
            if (pos + i >= contentSize || str[pos + i] != prefix[i]) return false;
            i++;
        }
        return true;
    }
    
    void extractText(const char* xml, long start, long end, char* result) const {
        int resIdx = 0;
        for (long i = start; i < end && resIdx < 510; i++) {
            if (xml[i] != '\n' && xml[i] != '\r') {
                result[resIdx++] = xml[i];
            }
        }
        result[resIdx] = '\0';
    }
    
public:
    SimpleXMLParser() : content(nullptr), contentSize(0) {}
    
    ~SimpleXMLParser() {
        if (content) delete[] content;
    }
    
    bool loadFile(const char* filename) {
        FILE* file = fopen(filename, "rb");
        if (!file) return false;
        
        fseek(file, 0, SEEK_END);
        contentSize = ftell(file);
        fseek(file, 0, SEEK_SET);
        
        content = new char[contentSize + 1];
        fread(content, 1, contentSize, file);
        content[contentSize] = '\0';
        
        fclose(file);
        return true;
    }
    
    void extractURLs(StringArray& urls) {
        long pos = 0;
        
        while (pos < contentSize) {
            
            if (startsWith(content, "<url>", pos)) {
                long start = pos + 5;
                long end = start;
                
                
                while (end < contentSize && !startsWith(content, "</url>", end)) {
                    end++;
                }
                
                if (end < contentSize) {
                    char url[512];
                    extractText(content, start, end, url);
                    urls.add(url);
                    pos = end + 6;
                } else {
                    break;
                }
            } else {
                pos++;
            }
        }
    }
};

/*
ФОРМАТ ФАЙЛА INDEX.BIN:

ЗАГОЛОВОК (HEADER):
[0-3]   MAGIC NUMBER: "SIDX" (4 байта)
[4-7]   VERSION: 1 (4 байта, uint32)
[8-11]  NUM_TERMS: количество уникальных термов (4 байта, uint32)
[12-15] NUM_DOCS: количество документов (4 байта, uint32)
[16-23] INVERTED_INDEX_OFFSET: смещение до инвертированного индекса (8 байт, uint64)
[24-31] FORWARD_INDEX_OFFSET: смещение до прямого индекса (8 байт, uint64)

ИНВЕРТИРОВАННЫЙ ИНДЕКС (начинается с INVERTED_INDEX_OFFSET):
Для каждого терма (отсортированы лексикографически):
  [0-1]   TERM_LENGTH: длина терма (2 байта, uint16)
  [2-N]   TERM: строка терма (TERM_LENGTH байт)
  [N+1-N+4] DOC_COUNT: количество документов (4 байта, uint32)
  [N+5...] DOC_IDS: список ID документов (DOC_COUNT * 4 байта, каждый uint32)

ПРЯМОЙ ИНДЕКС (начинается с FORWARD_INDEX_OFFSET):
Для каждого документа:
  [0-3]   DOC_ID: ID документа (4 байта, uint32)
  [4-5]   URL_LENGTH: длина URL (2 байта, uint16)
  [6-N]   URL: строка URL (URL_LENGTH байт)
  [N+1-N+4] TERM_COUNT: количество термов в документе (4 байта, uint32)
*/

class BinaryIndexWriter {
private:
    FILE* file;
    
    void writeUInt32(unsigned int value) {
        unsigned char bytes[4];
        bytes[0] = value & 0xFF;
        bytes[1] = (value >> 8) & 0xFF;
        bytes[2] = (value >> 16) & 0xFF;
        bytes[3] = (value >> 24) & 0xFF;
        fwrite(bytes, 1, 4, file);
    }
    
    void writeUInt64(unsigned long long value) {
        unsigned char bytes[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (value >> (i * 8)) & 0xFF;
        }
        fwrite(bytes, 1, 8, file);
    }
    
    void writeUInt16(unsigned short value) {
        unsigned char bytes[2];
        bytes[0] = value & 0xFF;
        bytes[1] = (value >> 8) & 0xFF;
        fwrite(bytes, 1, 2, file);
    }
    
    void writeString(const char* str, int maxLen = -1) {
        int len = 0;
        while (str[len] != '\0' && (maxLen == -1 || len < maxLen)) len++;
        fwrite(str, 1, len, file);
    }
    
public:
    BinaryIndexWriter() : file(nullptr) {}
    
    ~BinaryIndexWriter() {
        if (file) fclose(file);
    }
    
    bool open(const char* filename) {
        file = fopen(filename, "wb");
        if (!file) {
            std::cerr << "Ошибка создания файла индекса: " << filename << std::endl;
            return false;
        }
        return true;
    }
    
    void writeIndex(InvertedIndex& invIndex, ForwardIndex& fwdIndex) {
        std::cout << "\nЗапись бинарного индекса..." << std::endl;
        
        
        writeString("SIDX", 4);  
        writeUInt32(1);          
        writeUInt32(invIndex.getUniqueTerms());  
        writeUInt32(fwdIndex.getSize());         
        writeUInt64(32);  
        writeUInt64(0);   
        
        long long invertedIndexStart = 32;
        
        
        int termCount = invIndex.getUniqueTerms();
        TermEntry** allTerms = new TermEntry*[termCount];
        int count = 0;
        invIndex.getAllTerms(allTerms, count);
        
        std::cout << "Сортировка " << count << " термов..." << std::endl;
        quickSortTerms(allTerms, 0, count - 1);
        
        
        std::cout << "Запись инвертированного индекса..." << std::endl;
        for (int i = 0; i < count; i++) {
            TermEntry* entry = allTerms[i];
            
            
            int termLen = 0;
            while (entry->term[termLen] != '\0') termLen++;
            writeUInt16((unsigned short)termLen);
            
            
            writeString(entry->term, termLen);
            
            
            int docCount = entry->postings.docIds.getSize();
            writeUInt32(docCount);
            
            
            for (int j = 0; j < docCount; j++) {
                writeUInt32(entry->postings.docIds.get(j));
            }
            
            if ((i + 1) % 5000 == 0) {
                std::cout << "  Записано термов: " << (i + 1) << std::endl;
            }
        }
        
        delete[] allTerms;
        
        
        long long forwardIndexStart = ftell(file);
        
        
        std::cout << "Запись прямого индекса..." << std::endl;
        DocumentMetadata* docs = fwdIndex.getAllDocuments();
        for (int i = 0; i < fwdIndex.getSize(); i++) {
            writeUInt32(docs[i].docId);
            
            int urlLen = 0;
            while (docs[i].url[urlLen] != '\0') urlLen++;
            writeUInt16((unsigned short)urlLen);
            writeString(docs[i].url, urlLen);
            
            writeUInt32(docs[i].termCount);
        }
        
        
        fseek(file, 24, SEEK_SET);
        writeUInt64(forwardIndexStart);
        
        std::cout << "Индекс успешно записан!" << std::endl;
        std::cout << "  Размер файла: " << (forwardIndexStart + fwdIndex.getSize() * 520) / 1024 << " КБ" << std::endl;
    }
};

int main() {
    std::cout << "=== ПОСТРОЕНИЕ БУЛЕВА ИНДЕКСА ===" << std::endl;
    std::cout << std::endl;
    
    clock_t startTime = clock();
    
    
    
    
    
    std::cout << "Шаг 1: Загрузка метаданных документов из articles.xml..." << std::endl;
    
    SimpleXMLParser xmlParser;
    if (!xmlParser.loadFile("../lab2/articles.xml")) {
        std::cerr << "Не удалось загрузить articles.xml" << std::endl;
        return 1;
    }
    
    StringArray urls;
    xmlParser.extractURLs(urls);
    
    std::cout << "  Найдено документов: " << urls.getSize() << std::endl;
    
    
    
    
    
    std::cout << "\nШаг 2: Чтение токенов из tokens.csv..." << std::endl;
    
    CSVParser csvParser;
    if (!csvParser.open("../lab3-5/tokens.csv")) {
        return 1;
    }
    
    InvertedIndex invIndex;
    ForwardIndex fwdIndex;
    
    int currentDocId = -1;
    int termCountInDoc = 0;
    int docId;
    char token[256];
    int processedTokens = 0;
    long long totalTermLength = 0;
    
    while (csvParser.readNext(docId, token)) {
        
        if (docId != currentDocId) {
            
            if (currentDocId != -1) {
                const char* url = urls.get(currentDocId - 1);
                if (url) {
                    fwdIndex.addDocument(currentDocId, url, termCountInDoc);
                }
            }
            
            currentDocId = docId;
            termCountInDoc = 0;
        }
        
        
        invIndex.addTerm(token, docId);
        termCountInDoc++;
        processedTokens++;
        
        
        int len = 0;
        while (token[len] != '\0') len++;
        totalTermLength += len;
        
        if (processedTokens % 50000 == 0) {
            std::cout << "  Обработано токенов: " << processedTokens << std::endl;
        }
    }
    
    
    if (currentDocId != -1) {
        const char* url = urls.get(currentDocId - 1);
        if (url) {
            fwdIndex.addDocument(currentDocId, url, termCountInDoc);
        }
    }
    
    std::cout << "  Всего обработано токенов: " << processedTokens << std::endl;
    
    
    
    
    
    std::cout << "\nШаг 3: Финализация индекса (сортировка постинг-листов)..." << std::endl;
    invIndex.finalizeAllPostings();
    
    
    
    
    
    std::cout << "\nШаг 4: Запись бинарного индекса..." << std::endl;
    
    BinaryIndexWriter writer;
    if (!writer.open("index.bin")) {
        return 1;
    }
    
    writer.writeIndex(invIndex, fwdIndex);
    
    
    clock_t endTime = clock();
    double totalTime = (double)(endTime - startTime) / CLOCKS_PER_SEC;
    
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "=== СТАТИСТИКА ИНДЕКСАЦИИ ===" << std::endl;
    std::cout << std::string(70, '=') << std::endl;
    
    std::cout << "\nОСНОВНЫЕ ПОКАЗАТЕЛИ:" << std::endl;
    std::cout << "  Количество документов: " << fwdIndex.getSize() << std::endl;
    std::cout << "  Количество уникальных термов: " << invIndex.getUniqueTerms() << std::endl;
    std::cout << "  Общее количество токенов: " << invIndex.getTotalOccurrences() << std::endl;
    
    double avgTermLength = (double)totalTermLength / processedTokens;
    std::cout << "\n  Средняя длина терма: " << avgTermLength << " символов" << std::endl;
    
    std::cout << "\nПРОИЗВОДИТЕЛЬНОСТЬ:" << std::endl;
    std::cout << "  Общее время индексации: " << totalTime << " сек" << std::endl;
    std::cout << "  Время на 1 документ: " << (totalTime / fwdIndex.getSize() * 1000) << " мс" << std::endl;
    
    
    long estimatedDataSize = processedTokens * 10; 
    double kbPerSecond = (estimatedDataSize / 1024.0) / totalTime;
    std::cout << "  Скорость индексации: " << kbPerSecond << " КБ/сек" << std::endl;
    
    std::cout << "\nАНАЛИЗ МЕТОДА СОРТИРОВКИ:" << std::endl;
    std::cout << "  Использован алгоритм: QuickSort" << std::endl;
    std::cout << "  Сложность: O(n log n) в среднем, O(n²) в худшем" << std::endl;
    std::cout << "  Достоинства:" << std::endl;
    std::cout << "    + Быстрый на практике" << std::endl;
    std::cout << "    + In-place сортировка (не требует доп. памяти)" << std::endl;
    std::cout << "    + Хорошая локальность кэша" << std::endl;
    std::cout << "  Недостатки:" << std::endl;
    std::cout << "    - Нестабильная сортировка" << std::endl;
    std::cout << "    - Худший случай O(n²) на отсортированных данных" << std::endl;
    
    std::cout << "\nМАСШТАБИРУЕМОСТЬ:" << std::endl;
    std::cout << "  При увеличении данных в 10 раз:" << std::endl;
    std::cout << "    - Время индексации: ~" << (totalTime * 10) << " сек" << std::endl;
    std::cout << "    - Размер индекса: ~" << (invIndex.getUniqueTerms() * 5 / 1024) << " МБ" << std::endl;
    
    std::cout << "  При увеличении в 100 раз:" << std::endl;
    std::cout << "    - Время: ~" << (totalTime * 100 / 60) << " мин" << std::endl;
    std::cout << "    - Потребуется: внешняя сортировка, разбиение на части" << std::endl;
    
    std::cout << "  При увеличении в 1000 раз:" << std::endl;
    std::cout << "    - Не поместится в оперативную память" << std::endl;
    std::cout << "    - Решение: распределённая индексация (MapReduce)" << std::endl;
    
    std::cout << "\nОПТИМИЗАЦИИ:" << std::endl;
    std::cout << "  Текущие узкие места:" << std::endl;
    std::cout << "    - Последовательное чтение CSV (можно распараллелить)" << std::endl;
    std::cout << "    - Сортировка термов (можно использовать RadixSort для строк)" << std::endl;
    std::cout << "    - Запись в файл (можно буферизировать)" << std::endl;
    
    std::cout << "  Возможные улучшения:" << std::endl;
    std::cout << "    + Многопоточность при построении индекса" << std::endl;
    std::cout << "    + Использование mmap для больших файлов" << std::endl;
    std::cout << "    + Сжатие постинг-листов (Gap encoding, VByte)" << std::endl;
    std::cout << "    + Инкрементальная индексация" << std::endl;
    
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "Индексация завершена успешно!" << std::endl;
    std::cout << "Создан файл: index.bin" << std::endl;
    std::cout << std::string(70, '=') << std::endl;
    
    return 0;
}
