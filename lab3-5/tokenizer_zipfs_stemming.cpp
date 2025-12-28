#include <iostream>
#include <ctime>

int myStrlen(const char* str) {
    int len = 0;
    while (str[len] != '\0') len++;
    return len;
}

bool myStrcmp(const char* s1, const char* s2) {
    while (*s1 && *s2 && *s1 == *s2) {
        s1++;
        s2++;
    }
    return (*s1 == '\0' && *s2 == '\0');
}

int myStrncmp(const char* s1, const char* s2, int n) {
    for (int i = 0; i < n; i++) {
        if (s1[i] != s2[i]) return s1[i] - s2[i];
        if (s1[i] == '\0') return 0;
    }
    return 0;
}

void myStrcpy(char* dest, const char* src) {
    while (*src) {
        *dest++ = *src++;
    }
    *dest = '\0';
}

double myLog(double x) {
    if (x <= 0) return 0;
    
    double result = 0;
    double term = (x - 1) / (x + 1);
    double term_sq = term * term;
    double current = term;
    
    for (int i = 0; i < 20; i++) {
        result += current / (2 * i + 1);
        current *= term_sq;
    }
    
    return 2 * result;
}

double fabs(double x) {
    return (x < 0) ? -x : x;
}const int HASH_TABLE_SIZE = 50021;

unsigned int hashFunction(const char* str) {
    unsigned int hash = 5381;
    int c;
    
    while ((c = *str++)) {
        if (c >= 'A' && c <= 'Z') c = c + 32;
        hash = ((hash << 5) + hash) + c;
    }
    
    return hash % HASH_TABLE_SIZE;
}

struct TokenFreq {
    char* text;
    int length;
    int frequency;
    TokenFreq* next;
};

struct FreqPair {
    char* text;
    int freq;
};

class HashMap {
private:
    TokenFreq** table;
    int unique_count;
    int total_count;
    
public:
    HashMap() {
        table = new TokenFreq*[HASH_TABLE_SIZE];
        for (int i = 0; i < HASH_TABLE_SIZE; i++) {
            table[i] = nullptr;
        }
        unique_count = 0;
        total_count = 0;
    }
    
    ~HashMap() {
        for (int i = 0; i < HASH_TABLE_SIZE; i++) {
            TokenFreq* current = table[i];
            while (current) {
                TokenFreq* next = current->next;
                delete[] current->text;
                delete current;
                current = next;
            }
        }
        delete[] table;
    }
    
    void addToken(const char* token_text, int token_len) {
        unsigned int index = hashFunction(token_text);
        
        TokenFreq* current = table[index];
        while (current) {
            if (myStrcmp(current->text, token_text)) {
                current->frequency++;
                total_count++;
                return;
            }
            current = current->next;
        }
        
        TokenFreq* new_token = new TokenFreq;
        new_token->text = new char[token_len + 1];
        myStrcpy(new_token->text, token_text);
        new_token->length = token_len;
        new_token->frequency = 1;
        
        new_token->next = table[index];
        table[index] = new_token;
        
        unique_count++;
        total_count++;
    }
    
    int getUniqueCount() const {
        return unique_count;
    }
    
    int getTotalCount() const {
        return total_count;
    }
    
    FreqPair* toArray() {
        FreqPair* array = new FreqPair[unique_count];
        int index = 0;
        
        for (int i = 0; i < HASH_TABLE_SIZE; i++) {
            TokenFreq* current = table[i];
            while (current) {
                array[index].text = new char[current->length + 1];
                myStrcpy(array[index].text, current->text);
                array[index].freq = current->frequency;
                
                current = current->next;
                index++;
            }
        }
        
        return array;
    }
};class RussianStemmer {
private:
    char buffer[256];
    int len;
    
    void copy(const char* str) {
        len = 0;
        while (str[len] != '\0' && len < 255) {
            buffer[len] = str[len];
            len++;
        }
        buffer[len] = '\0';
    }
    
    bool removeIfEndsWith(const char* ending) {
        int endLen = 0;
        while (ending[endLen] != '\0') endLen++;
        
        if (len < endLen) return false;
        
        bool match = true;
        for (int i = 0; i < endLen; i++) {
            if (buffer[len - endLen + i] != ending[i]) {
                match = false;
                break;
            }
        }
        
        if (match && len - endLen >= 3) {
            len -= endLen;
            buffer[len] = '\0';
            return true;
        }
        return false;
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
        if (removeIfEndsWith("\xD1\x8F\xD1\x8f")) return buffer;
        
        
        if (removeIfEndsWith("\xD0\xBE\xD0\xB5")) return buffer;
        if (removeIfEndsWith("\xD0\xB5\xD0\xB5")) return buffer;
        
        
        if (removeIfEndsWith("\xD1\x8B\xD0\xB5")) return buffer;
        if (removeIfEndsWith("\xD0\xB8\xD0\xB5")) return buffer;
        
        
        if (removeIfEndsWith("\xD1\x82\xD1\x8C")) return buffer;
        
        
        if (removeIfEndsWith("\xD0\xB5\xD1\x82")) return buffer;
        if (removeIfEndsWith("\xD0\xB8\xD1\x82")) return buffer;
        if (removeIfEndsWith("\xD1\x8E\xD1\x82")) return buffer;
        if (removeIfEndsWith("\xD1\x8F\xD1\x82")) return buffer;
        
        
        if (removeIfEndsWith("\xD0\xB0\xD0\xBB")) return buffer;
        if (removeIfEndsWith("\xD0\xB5\xD0\xBB")) return buffer;
        if (removeIfEndsWith("\xD0\xB8\xD0\xBB")) return buffer;
        
        
        if (removeIfEndsWith("\xD1\x83")) return buffer;
        if (removeIfEndsWith("\xD1\x8E")) return buffer;
        if (removeIfEndsWith("\xD0\xB0")) return buffer;
        if (removeIfEndsWith("\xD1\x8F")) return buffer;
        if (removeIfEndsWith("\xD1\x8B")) return buffer;
        if (removeIfEndsWith("\xD0\xB8")) return buffer;
        if (removeIfEndsWith("\xD0\xBE")) return buffer;
        if (removeIfEndsWith("\xD0\xB5")) return buffer;
        
        
        if (removeIfEndsWith("ing")) return buffer;
        if (removeIfEndsWith("ed")) return buffer;
        if (removeIfEndsWith("ly")) return buffer;
        if (removeIfEndsWith("er")) return buffer;
        if (removeIfEndsWith("s")) return buffer;
        
        return buffer;
    }
};

int partition(FreqPair* array, int low, int high) {
    int pivot = array[high].freq;
    int i = low - 1;
    
    for (int j = low; j < high; j++) {
        if (array[j].freq > pivot) {
            i++;
            FreqPair temp = array[i];
            array[i] = array[j];
            array[j] = temp;
        }
    }
    
    FreqPair temp = array[i + 1];
    array[i + 1] = array[high];
    array[high] = temp;
    
    return i + 1;
}

void quickSort(FreqPair* array, int low, int high) {
    if (low < high) {
        int pi = partition(array, low, high);
        quickSort(array, low, pi - 1);
        quickSort(array, pi + 1, high);
    }
}

void sortFreqArray(FreqPair* array, int size) {
    if (size > 1) {
        quickSort(array, 0, size - 1);
    }
}char* extractContentFromXML(const char* xml_content, int xml_size, int& extracted_size) {
    char* text = new char[xml_size + 1];
    int text_pos = 0;
    
    const char* ptr = xml_content;
    const char* end = xml_content + xml_size;
    
    while (ptr < end) {
        if (myStrncmp(ptr, "<content>", 9) == 0) {
            ptr += 9;
            
            while (ptr < end && (*ptr == ' ' || *ptr == '\n' || *ptr == '\t' || *ptr == '\r')) {
                ptr++;
            }
            
            if (myStrncmp(ptr, "&lt;![CDATA[", 11) == 0) {
                ptr += 11;
                
                while (ptr < end && !(myStrncmp(ptr, "]]&gt;", 4) == 0)) {
                    if (myStrncmp(ptr, "&quot;", 6) == 0) {
                        text[text_pos++] = '"';
                        ptr += 6;
                    } else if (myStrncmp(ptr, "&amp;", 5) == 0) {
                        text[text_pos++] = '&';
                        ptr += 5;
                    } else if (myStrncmp(ptr, "&lt;", 4) == 0) {
                        text[text_pos++] = '<';
                        ptr += 4;
                    } else if (myStrncmp(ptr, "&gt;", 4) == 0) {
                        text[text_pos++] = '>';
                        ptr += 4;
                    } else {
                        text[text_pos++] = *ptr++;
                    }
                }
                
                if (ptr < end) {
                    ptr += 4;
                }
            }
            
            text[text_pos++] = ' ';
        } else {
            ptr++;
        }
    }
    
    text[text_pos] = '\0';
    extracted_size = text_pos;
    
    return text;
}

char* readFile(const char* filename, int& size) {
    FILE* file = fopen(filename, "rb");
    if (!file) {
        std::cerr << "Не могу открыть файл: " << filename << std::endl;
        return nullptr;
    }
    
    fseek(file, 0, SEEK_END);
    size = ftell(file);
    fseek(file, 0, SEEK_SET);
    
    char* buffer = new char[size + 1];
    fread(buffer, 1, size, file);
    buffer[size] = '\0';
    
    fclose(file);
    return buffer;
}

void toLowerCase(char* str) {
    for (int i = 0; str[i] != '\0'; i++) {
        unsigned char c = (unsigned char)str[i];
        
        
        if (c >= 'A' && c <= 'Z') {
            str[i] = c + 32;
        }
        
        else if (c == 0xD0) {
            unsigned char next = (unsigned char)str[i + 1];
            
            if (next >= 0x90 && next <= 0x9F) {
                str[i + 1] = next + 0x20;
                i++;
            }
            
            
            
            
            else if (next >= 0xA0 && next <= 0xAF) {
                str[i] = 0xD1;  
                str[i + 1] = next - 0x20;  
                i++;
            }
            
            else if (next == 0x81) {
                str[i] = 0xD1;
                str[i + 1] = 0x91;
                i++;
            }
        }
    }
}

bool isDelimiter(char c) {
    return c == ' ' || c == '\n' || c == '\t' || c == '\r' || 
           c == ',' || c == '.' || c == '!' || c == '?' || 
           c == ';' || c == ':' || c == '(' || c == ')' ||
           c == '[' || c == ']' || c == '"' || c == '\'' ||
           c == '-' || c == '_' || c == '/' || c == '\\';
}

bool isJunkToken(const char* token) {
    const char* junk_tokens[] = {
        "http", "https", "www", "html", "xml", 
        "url", "content", "statistics", "character_count",
        "word_count", "article", "source", "id", "meta",
        "total_articles", "generated_date", "cdata", 
        "&lt", "&gt", "&amp", "&quot", "]]&gt", "<![cdata[",
        "f1news", "ru", "news", "f1", NULL
    };
    
    for (int i = 0; junk_tokens[i] != NULL; i++) {
        if (myStrcmp(token, junk_tokens[i])) {
            return true;
        }
    }
    
    if (token[0] == 'h' && token[1] == 't' && token[2] == 't' && token[3] == 'p')
        return true;
    if (token[0] == 'w' && token[1] == 'w' && token[2] == 'w')
        return true;
    
    if (myStrlen(token) == 1) {
        char c = token[0];
        if (!((c >= 'а' && c <= 'я') || (c >= 'a' && c <= 'z') || 
              c == 'ё' || c == 'й' || c == '-')) {
            return true;
        }
    }
    
    return false;
}

void tokenizeText(const char* text, HashMap& hashmap) {
    int i = 0;
    int len = myStrlen(text);
    
    while (i < len) {
        while (i < len && isDelimiter(text[i])) {
            i++;
        }
        
        if (i >= len) break;
        
        int start = i;
        
        while (i < len && !isDelimiter(text[i])) {
            i++;
        }
        
        int token_len = i - start;
        if (token_len > 0 && token_len < 50) {
            char* token_text = new char[token_len + 1];
            for (int j = 0; j < token_len; j++) {
                token_text[j] = text[start + j];
            }
            token_text[token_len] = '\0';
            
            toLowerCase(token_text);
            
            if (!isJunkToken(token_text)) {
                hashmap.addToken(token_text, token_len);
            }
            
            delete[] token_text;
        }
    }
}

void tokenizeWithStemming(const char* text, HashMap& hashmap, RussianStemmer& stemmer) {
    int i = 0;
    int len = myStrlen(text);
    
    while (i < len) {
        while (i < len && isDelimiter(text[i])) {
            i++;
        }
        
        if (i >= len) break;
        
        int start = i;
        
        while (i < len && !isDelimiter(text[i])) {
            i++;
        }
        
        int token_len = i - start;
        if (token_len > 0 && token_len < 50) {
            char* token_text = new char[token_len + 1];
            for (int j = 0; j < token_len; j++) {
                token_text[j] = text[start + j];
            }
            token_text[token_len] = '\0';
            
            toLowerCase(token_text);
            
            if (!isJunkToken(token_text)) {
                const char* stem = stemmer.stem(token_text);
                hashmap.addToken(stem, myStrlen(stem));
            }
            
            delete[] token_text;
        }
    }
}

double calculateZipfConstant(int total_tokens, int unique_tokens) {
    double harmonic = 0;
    for (int i = 1; i <= unique_tokens; i++) {
        harmonic += 1.0 / i;
    }
    return total_tokens / harmonic;
}

void analyzeZipfLaw(FreqPair* freq_array, int unique_tokens, int total_tokens, const char* title) {
    std::cout << "\n=== " << title << " ===" << std::endl;
    
    double C = calculateZipfConstant(total_tokens, unique_tokens);
    std::cout << "Константа Ципфа C = " << (int)C << std::endl;
    
    std::cout << "\nСРАВНЕНИЕ РЕАЛЬНЫХ И ПРЕДСКАЗАННЫХ ЧАСТОТ:" << std::endl;
    std::cout << "Ранг\tТокен\t\t\tРеальная\tЦипф\tОтклонение\tlog(Ранг)\tlog(Частота)" << std::endl;
    std::cout << "---------------------------------------------------------------------------------" << std::endl;
    
    double total_deviation = 0;
    int compare_count = (unique_tokens < 50) ? unique_tokens : 50;
    
    for (int i = 0; i < compare_count; i++) {
        int rank = i + 1;
        int real_freq = freq_array[i].freq;
        double zipf_freq = C / rank;
        double deviation = (real_freq - zipf_freq) / zipf_freq * 100;
        total_deviation += fabs(deviation);
        
        double log_rank = myLog((double)rank);
        double log_freq = myLog((double)real_freq);
        
        std::cout << rank << "\t" << freq_array[i].text;
        
        int token_len = myStrlen(freq_array[i].text);
        if (token_len < 8) std::cout << "\t\t\t";
        else if (token_len < 16) std::cout << "\t\t";
        else std::cout << "\t";
        
        std::cout << real_freq << "\t" << (int)zipf_freq << "\t" 
                  << deviation << "%\t" << log_rank << "\t" << log_freq << std::endl;
    }
    
    std::cout << "\nСредняя абсолютная ошибка: " 
              << total_deviation / compare_count << "%" << std::endl;
}

void printTopTokens(FreqPair* freq_array, int unique_tokens, int total_tokens, const char* title) {
    std::cout << "\n=== " << title << " ===" << std::endl;
    std::cout << "Ранг\tТокен\t\t\tЧастота\t%" << std::endl;
    std::cout << "--------------------------------------------------------" << std::endl;
    
    int show_count = (unique_tokens < 30) ? unique_tokens : 30;
    for (int i = 0; i < show_count; i++) {
        double percentage = (double)freq_array[i].freq / total_tokens * 100;
        
        std::cout << (i+1) << "\t" << freq_array[i].text;
        
        int token_len = myStrlen(freq_array[i].text);
        if (token_len < 8) std::cout << "\t\t\t";
        else if (token_len < 16) std::cout << "\t\t";
        else std::cout << "\t";
        
        std::cout << freq_array[i].freq << "\t" << percentage << "%" << std::endl;
    }
}

void demonstrateStemming() {
    std::cout << "\n=== ПРИМЕРЫ РАБОТЫ СТЕММЕРА ===" << std::endl;
    
    RussianStemmer stemmer;
    
    const char* examples[] = {
        "гонщики", "гонщиков", "гонщикам",
        "команда", "команды", "командами",
        "чемпионат", "чемпионата", "чемпионате",
        "побед", "победа", "победы", "победами",
        "этап", "этапа", "этапов", "этапами",
        "машина", "машины", "машинами",
        "трасса", "трассы", "трассе",
        "racing", "races", "raced",
        "driver", "drivers", "driving"
    };
    
    int numExamples = sizeof(examples) / sizeof(examples[0]);
    
    std::cout << "\nСлово\t\t→\tОснова" << std::endl;
    std::cout << "-----\t\t \t------" << std::endl;
    
    for (int i = 0; i < numExamples; i++) {
        const char* stemmed = stemmer.stem(examples[i]);
        std::cout << examples[i] << "\t\t→\t" << stemmed << std::endl;
    }
}


int extractArticleId(const char* xml, int start_pos, int xml_size) {
    int pos = start_pos;
    
    
    int search_limit = pos + 100;
    if (search_limit > xml_size) search_limit = xml_size;
    
    while (pos < search_limit) {
        if (myStrncmp(xml + pos, "id=\"", 4) == 0) {
            pos += 4; 
            
            
            int id = 0;
            while (pos < xml_size && xml[pos] >= '0' && xml[pos] <= '9') {
                id = id * 10 + (xml[pos] - '0');
                pos++;
            }
            
            return id;
        }
        pos++;
    }
    
    return -1; 
}

void saveTokensForIndexing(const char* xml_content, int xml_size, RussianStemmer& stemmer, const char* outputFile) {
    FILE* file = fopen(outputFile, "wb");
    if (!file) {
        std::cerr << "Ошибка создания файла " << outputFile << std::endl;
        return;
    }
    
    unsigned char bom[] = {0xEF, 0xBB, 0xBF};
    fwrite(bom, 1, 3, file);
    
    fprintf(file, "doc_id,token\n");
    
    const char* ptr = xml_content;
    const char* end = xml_content + xml_size;
    int documents_processed = 0;
    
    while (ptr < end) {
        
        if (myStrncmp(ptr, "<article", 8) == 0) {
            
            int doc_id = extractArticleId(xml_content, ptr - xml_content, xml_size);
            
            if (doc_id == -1) {
                ptr++;
                continue; 
            }
            
            
            const char* content_start = ptr;
            while (content_start < end && myStrncmp(content_start, "<content>", 9) != 0) {
                content_start++;
                
                if (myStrncmp(content_start, "<article", 8) == 0) {
                    break;
                }
            }
            
            if (content_start >= end || myStrncmp(content_start, "<content>", 9) != 0) {
                ptr++;
                continue; 
            }
            
            content_start += 9; 
            
            
            const char* content_end = content_start;
            while (content_end < end && myStrncmp(content_end, "</content>", 10) != 0) {
                content_end++;
            }
            
            
            const char* doc_ptr = content_start;
            
            while (doc_ptr < content_end) {
                
                if (myStrncmp(doc_ptr, "&lt;![CDATA[", 12) == 0) {
                    doc_ptr += 12;
                    continue;
                }
                if (myStrncmp(doc_ptr, "]]&gt;", 6) == 0) {
                    doc_ptr += 6;
                    continue;
                }
                if (myStrncmp(doc_ptr, "<![CDATA[", 9) == 0) {
                    doc_ptr += 9;
                    continue;
                }
                if (myStrncmp(doc_ptr, "]]>", 3) == 0) {
                    doc_ptr += 3;
                    continue;
                }
                
                
                if (myStrncmp(doc_ptr, "&quot;", 6) == 0) {
                    doc_ptr += 6;
                    continue;
                }
                if (myStrncmp(doc_ptr, "&amp;", 5) == 0) {
                    doc_ptr += 5;
                    continue;
                }
                if (myStrncmp(doc_ptr, "&lt;", 4) == 0) {
                    doc_ptr += 4;
                    continue;
                }
                if (myStrncmp(doc_ptr, "&gt;", 4) == 0) {
                    doc_ptr += 4;
                    continue;
                }
                
                
                if (isDelimiter(*doc_ptr)) {
                    doc_ptr++;
                    continue;
                }
                
                
                const char* token_start = doc_ptr;
                
                while (doc_ptr < content_end && !isDelimiter(*doc_ptr)) {
                    
                    if (*doc_ptr == '&' || *doc_ptr == '<' || *doc_ptr == ']') {
                        break;
                    }
                    doc_ptr++;
                }
                
                int token_len = doc_ptr - token_start;
                if (token_len > 0 && token_len < 50) {
                    char* token_text = new char[token_len + 1];
                    for (int j = 0; j < token_len; j++) {
                        token_text[j] = token_start[j];
                    }
                    token_text[token_len] = '\0';
                    
                    toLowerCase(token_text);
                    
                    if (!isJunkToken(token_text)) {
                        const char* stem = stemmer.stem(token_text);
                        fprintf(file, "%d,%s\n", doc_id, stem);
                    }
                    
                    delete[] token_text;
                }
            }
            
            documents_processed++;
            if (documents_processed % 1000 == 0) {
                std::cout << "Обработано документов: " << documents_processed << std::endl;
            }
            
            
            ptr = content_end + 10; 
        } else {
            ptr++;
        }
    }
    
    fclose(file);
    std::cout << "Токены из " << documents_processed << " документов сохранены в " << outputFile << std::endl;
}
void freeFreqArray(FreqPair* array, int size) {
    for (int i = 0; i < size; i++) {
        delete[] array[i].text;
    }
    delete[] array;
}


int main() {
    std::cout << "=== ТОКЕНИЗАЦИЯ И СТЕММИНГ ===" << std::endl;
    
    
    demonstrateStemming();
    
    
    std::cout << "\n1. ЧТЕНИЕ ФАЙЛА" << std::endl;
    
    int file_size;
    char* xml_content = readFile("../lab2/articles.xml", file_size);
    if (!xml_content) {
        return 1;
    }
    
    std::cout << "Размер XML файла: " << file_size << " байт" << std::endl;
    
    
    std::cout << "\n2. ИЗВЛЕЧЕНИЕ ТЕКСТА ИЗ CONTENT" << std::endl;
    
    int extracted_size;
    char* text = extractContentFromXML(xml_content, file_size, extracted_size);
    
    delete[] xml_content;
    
    std::cout << "Извлечено текста: " << extracted_size << " байт" << std::endl;
    
    if (extracted_size == 0) {
        std::cerr << "Ошибка: не удалось извлечь текст из XML!" << std::endl;
        return 1;
    }
    
    
    std::cout << "\n3. ТОКЕНИЗАЦИЯ БЕЗ СТЕММИНГА" << std::endl;
    
    clock_t start_original = clock();
    
    HashMap hashmap_original;
    tokenizeText(text, hashmap_original);
    
    clock_t end_original = clock();
    double time_original = (double)(end_original - start_original) / CLOCKS_PER_SEC;
    
    int total_original = hashmap_original.getTotalCount();
    int unique_original = hashmap_original.getUniqueCount();
    
    std::cout << "Всего токенов: " << total_original << std::endl;
    std::cout << "Уникальных токенов: " << unique_original << std::endl;
    std::cout << "Время: " << time_original << " сек" << std::endl;
    
    
    std::cout << "\n4. ТОКЕНИЗАЦИЯ СО СТЕММИНГОМ" << std::endl;
    
    clock_t start_stemmed = clock();
    
    HashMap hashmap_stemmed;
    RussianStemmer stemmer;
    tokenizeWithStemming(text, hashmap_stemmed, stemmer);
    
    clock_t end_stemmed = clock();
    double time_stemmed = (double)(end_stemmed - start_stemmed) / CLOCKS_PER_SEC;
    
    int total_stemmed = hashmap_stemmed.getTotalCount();
    int unique_stemmed = hashmap_stemmed.getUniqueCount();
    
    std::cout << "Всего токенов: " << total_stemmed << std::endl;
    std::cout << "Уникальных токенов: " << unique_stemmed << std::endl;
    std::cout << "Время: " << time_stemmed << " сек" << std::endl;
    
    
    std::cout << "\n5. СРАВНЕНИЕ: БЕЗ СТЕММИНГА vs СО СТЕММИНГОМ" << std::endl;
    std::cout << "========================================" << std::endl;
    
    std::cout << "\nБез стемминга:" << std::endl;
    std::cout << "  Общее количество токенов: " << total_original << std::endl;
    std::cout << "  Уникальных токенов: " << unique_original << std::endl;
    
    std::cout << "\nСо стеммингом:" << std::endl;
    std::cout << "  Общее количество токенов: " << total_stemmed << std::endl;
    std::cout << "  Уникальных токенов: " << unique_stemmed << std::endl;
    
    int reduction = unique_original - unique_stemmed;
    double reduction_percent = (reduction * 100.0) / unique_original;
    
    std::cout << "\nСокращение уникальных токенов: " << reduction 
              << " (" << reduction_percent << "%)" << std::endl;
    
    
    FreqPair* freq_original = hashmap_original.toArray();
    sortFreqArray(freq_original, unique_original);
    
    FreqPair* freq_stemmed = hashmap_stemmed.toArray();
    sortFreqArray(freq_stemmed, unique_stemmed);
    
    
    printTopTokens(freq_original, unique_original, total_original, 
                   "ТОП-30 ТОКЕНОВ БЕЗ СТЕММИНГА");
    
    printTopTokens(freq_stemmed, unique_stemmed, total_stemmed, 
                   "ТОП-30 ТОКЕНОВ СО СТЕММИНГОМ");
    
    
    std::cout << "\n=== ПРОИЗВОДИТЕЛЬНОСТЬ ===" << std::endl;
    std::cout << "Время токенизации БЕЗ стемминга: " << time_original << " сек" << std::endl;
    std::cout << "Время токенизации СО стеммингом: " << time_stemmed << " сек" << std::endl;
    std::cout << "Замедление из-за стемминга: " 
              << ((time_stemmed - time_original) / time_original * 100.0) << "%" << std::endl;
    
    
    analyzeZipfLaw(freq_stemmed, unique_stemmed, total_stemmed, 
                   "ЗАКОН ЦИПФА (стеммированные токены)");
    
    
    std::cout << "\n=== ОЦЕНКА КАЧЕСТВА ЗАКОНА ЦИПФА ===" << std::endl;
    
    double C = calculateZipfConstant(total_stemmed, unique_stemmed);
    double total_deviation = 0;
    int compare_count = (unique_stemmed < 50) ? unique_stemmed : 50;
    
    for (int i = 0; i < compare_count; i++) {
        int rank = i + 1;
        int real_freq = freq_stemmed[i].freq;
        double zipf_freq = C / rank;
        double deviation = (real_freq - zipf_freq) / zipf_freq * 100;
        total_deviation += fabs(deviation);
    }
    
    double avg_deviation = total_deviation / compare_count;
    
    if (avg_deviation < 20) {
        std::cout << "ОТЛИЧНО - распределение хорошо соответствует закону Ципфа" << std::endl;
    } else if (avg_deviation < 40) {
        std::cout << "ХОРОШО - есть некоторые отклонения, но в целом соответствует" << std::endl;
    } else if (avg_deviation < 60) {
        std::cout << " УДОВЛЕТВОРИТЕЛЬНО - заметные отклонения от закона Ципфа" << std::endl;
    } else {
        std::cout << " ПЛОХО - распределение значительно отличается от закона Ципфа" << std::endl;
    }
    
    std::cout << "\nВОЗМОЖНЫЕ ПРИЧИНЫ ОТКЛОНЕНИЙ:" << std::endl;
    std::cout << "1. Тематическая специфика корпуса (F1 - узкая тема)" << std::endl;
    std::cout << "2. Много имен собственных (гонщики, команды, трассы)" << std::endl;
    std::cout << "3. Технические термины с высокой частотой" << std::endl;
    std::cout << "4. Ограниченный размер корпуса" << std::endl;
    std::cout << "5. Особенности языка (служебные слова)" << std::endl;
    
    
    std::cout << "\n=== СОХРАНЕНИЕ ТОКЕНОВ ДЛЯ ИНДЕКСАЦИИ ===" << std::endl;
    int xml_size_for_save;
    char* xml_for_save = readFile("../lab2/articles.xml", xml_size_for_save);
    saveTokensForIndexing(xml_for_save, xml_size_for_save, stemmer, "tokens.csv");
    delete[] xml_for_save;
    
    
    freeFreqArray(freq_original, unique_original);
    freeFreqArray(freq_stemmed, unique_stemmed);
    delete[] text;
    
    std::cout << "\n=== АНАЛИЗ ЗАВЕРШЕН ===" << std::endl;
    
    return 0;
}