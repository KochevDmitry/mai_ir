#include <iostream>


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

void myStrcpy(char* dest, const char* src) {
    while (*src) {
        *dest++ = *src++;
    }
    *dest = '\0';
}

int myStrncmp(const char* s1, const char* s2, int n) {
    for (int i = 0; i < n; i++) {
        if (s1[i] != s2[i]) return s1[i] - s2[i];
        if (s1[i] == '\0') return 0;
    }
    return 0;
}

const int HASH_TABLE_SIZE = 50021; 

unsigned int hashFunction(const char* str) {
    unsigned int hash = 5381; 
    int c;
    
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; 
    }
    
    return hash % HASH_TABLE_SIZE;
}struct TokenFreq {
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
    
public:
    HashMap() {
        table = new TokenFreq*[HASH_TABLE_SIZE];
        for (int i = 0; i < HASH_TABLE_SIZE; i++) {
            table[i] = nullptr;
        }
        unique_count = 0;
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
    }
    
    int getUniqueCount() const {
        return unique_count;
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
    
    
    double getAverageLength(int total_tokens) {
        long long total_length = 0;
        
        for (int i = 0; i < HASH_TABLE_SIZE; i++) {
            TokenFreq* current = table[i];
            while (current) {
                total_length += (long long)current->length * current->frequency;
                current = current->next;
            }
        }
        
        return (double)total_length / total_tokens;
    }
};int partition(FreqPair* array, int low, int high) {
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
}bool isDelimiter(char c) {
    return c == ' ' || c == '\n' || c == '\t' || c == '\r' || 
           c == ',' || c == '.' || c == '!' || c == '?' || 
           c == ';' || c == ':' || c == '(' || c == ')' ||
           c == '[' || c == ']' || c == '"' || c == '\'' ||
           c == '-' || c == '_' || c == '/' || c == '\\';
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
        
        
        else if (c == 0xD0 && i + 1 < myStrlen(str)) {
            unsigned char next = (unsigned char)str[i + 1];
            
            
            if (next >= 0x90 && next <= 0x9F) {
                str[i + 1] = next + 0x20;
                i++; 
            }
            
            else if (next >= 0xA0 && next <= 0xAF) {
                str[i + 1] = next + 0x20;
                i++;
            }
        }
        
        else if (c == 0xD0 && i + 1 < myStrlen(str)) {
            unsigned char next = (unsigned char)str[i + 1];
            if (next == 0x81) {
                str[i] = 0xD1;
                str[i + 1] = 0x91;
                i++;
            }
        }
    }
}

void tokenizeWithHashMap(const char* text, HashMap& hashmap, int& total_tokens) {
    total_tokens = 0;
    
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
        if (token_len > 0) {
            
            char* token_text = new char[token_len + 1];
            for (int j = 0; j < token_len; j++) {
                token_text[j] = text[start + j];
            }
            token_text[token_len] = '\0';
            
            
            toLowerCase(token_text);
            
            
             if (!isJunkToken(token_text)) {
                
                hashmap.addToken(token_text, token_len);
                total_tokens++;
            }
            
            delete[] token_text;
            total_tokens++;
        }
    }
}
void saveTokenList(FreqPair* freq_array, int unique_tokens, const char* filename) {
    FILE* file = fopen(filename, "w");
    if (!file) {
        std::cerr << "Ошибка создания файла: " << filename << std::endl;
        return;
    }
    
    for (int i = 0; i < unique_tokens; i++) {
        fprintf(file, "%s\n", freq_array[i].text);
    }
    
    fclose(file);
    std::cout << "Список токенов сохранен в файл: " << filename << std::endl;
}

char* extractContentFromXML(const char* xml_content, int xml_size, int& extracted_size) {
    
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


void freeFreqArray(FreqPair* array, int size) {
    for (int i = 0; i < size; i++) {
        delete[] array[i].text;
    }
    delete[] array;
}


int main() {
    std::cout << "=== ТОКЕНИЗАЦИЯ И ПОДСЧЕТ ЧАСТОТ (ОПТИМИЗИРОВАННАЯ) ===" << std::endl;
    std::cout << "Используется: хеш-таблица + QuickSort" << std::endl;
    
    
    int file_size;
    char* xml_content = readFile("../lab2/articles.xml", file_size);
    if (!xml_content) {
        return 1;
    }
    
    std::cout << "\n1. ЧТЕНИЕ ФАЙЛА" << std::endl;
    std::cout << "Размер файла: " << file_size << " байт (" 
              << file_size / 1024 << " КБ)" << std::endl;
    
    
    std::cout << "\n2. ИЗВЛЕЧЕНИЕ ТЕКСТА" << std::endl;
    int extracted_size;
    char* text = extractContentFromXML(xml_content, file_size, extracted_size);
    
    delete[] xml_content;
    
    std::cout << "Извлечено текста: " << extracted_size << " байт" << std::endl;
    
    
    std::cout << "\n3. ТОКЕНИЗАЦИЯ (с хеш-таблицей)" << std::endl;
    
    HashMap hashmap;
    int total_tokens;
    
    tokenizeWithHashMap(text, hashmap, total_tokens);
    
    delete[] text;
    
    int unique_tokens = hashmap.getUniqueCount();
    
    std::cout << "Всего токенов: " << total_tokens << std::endl;
    std::cout << "Уникальных токенов: " << unique_tokens << std::endl;
    std::cout << "Коэффициент уникальности: " 
              << (double)unique_tokens / total_tokens * 100 << "%" << std::endl;
    
    
    std::cout << "\n4. СОРТИРОВКА (QuickSort)" << std::endl;
    
    FreqPair* freq_array = hashmap.toArray();
    sortFreqArray(freq_array, unique_tokens);
    
    std::cout << "Токены отсортированы (сложность O(n log n))" << std::endl;
    
    
    std::cout << "\n5. ТОП-50 САМЫХ ЧАСТЫХ ТОКЕНОВ" << std::endl;
    std::cout << "Ранг\tТокен\t\t\tЧастота\t%" << std::endl;
    std::cout << "--------------------------------------------------------" << std::endl;
    
    int show_count = (unique_tokens < 50) ? unique_tokens : 50;
    for (int i = 0; i < show_count; i++) {
        double percentage = (double)freq_array[i].freq / total_tokens * 100;
        
        
        std::cout << (i+1) << "\t" << freq_array[i].text;
        
        
        int token_len = myStrlen(freq_array[i].text);
        if (token_len < 8) std::cout << "\t\t\t";
        else if (token_len < 16) std::cout << "\t\t";
        else std::cout << "\t";
        
        std::cout << freq_array[i].freq << "\t" << percentage << "%" << std::endl;
    }
    
    
    std::cout << "\n6. РАСПРЕДЕЛЕНИЕ ПО ЧАСТОТАМ" << std::endl;
    
    int max_freq = freq_array[0].freq;
    int* freq_dist = new int[max_freq + 1]();
    
    for (int i = 0; i < unique_tokens; i++) {
        if (freq_array[i].freq <= max_freq) {
            freq_dist[freq_array[i].freq]++;
        }
    }
    
    std::cout << "Частота\tКоличество токенов" << std::endl;
    std::cout << "-----------------------------" << std::endl;
    
    int max_show = (max_freq < 30) ? max_freq : 30;
    for (int i = 1; i <= max_show; i++) {
        if (freq_dist[i] > 0) {
            std::cout << i << "\t" << freq_dist[i] << std::endl;
        }
    }

    saveTokenList(freq_array, unique_tokens, "tokens_list.txt");

    
    
    std::cout << "\n7. ОБЩАЯ СТАТИСТИКА" << std::endl;
    
    
    int hapax_count = freq_dist[1];
    double hapax_percent = (double)hapax_count / unique_tokens * 100;
    
    std::cout << "Hapax legomena (токены встречающиеся 1 раз): " << hapax_count 
              << " (" << hapax_percent << "%)" << std::endl;
    
    
    int top10_coverage = 0;
    for (int i = 0; i < 10 && i < unique_tokens; i++) {
        top10_coverage += freq_array[i].freq;
    }
    double top10_percent = (double)top10_coverage / total_tokens * 100;
    
    std::cout << "Покрытие топ-10 токенов: " << top10_percent << "%" << std::endl;
    
    if (unique_tokens >= 100) {
        int top100_coverage = 0;
        for (int i = 0; i < 100; i++) {
            top100_coverage += freq_array[i].freq;
        }
        double top100_percent = (double)top100_coverage / total_tokens * 100;
        std::cout << "Покрытие топ-100 токенов: " << top100_percent << "%" << std::endl;
    }
    
    
    double avg_length = hashmap.getAverageLength(total_tokens);
    std::cout << "Средняя длина токена: " << avg_length << " символов" << std::endl;
    
    
    std::cout << "\n8. АНАЛИЗ ПРОИЗВОДИТЕЛЬНОСТИ" << std::endl;
    std::cout << "Размер хеш-таблицы: " << HASH_TABLE_SIZE << std::endl;
    std::cout << "Средняя длина цепочки: " 
              << (double)unique_tokens / HASH_TABLE_SIZE << std::endl;
    std::cout << "Коэффициент заполнения: " 
              << (double)unique_tokens / HASH_TABLE_SIZE * 100 << "%" << std::endl;
    
    std::cout << "\nСЛОЖНОСТЬ АЛГОРИТМОВ:" << std::endl;
    std::cout << "- Токенизация: O(n) где n - размер текста" << std::endl;
    std::cout << "- Добавление в хеш-таблицу: O(1) в среднем" << std::endl;
    std::cout << "- QuickSort: O(m log m) где m - уникальных токенов" << std::endl;
    std::cout << "- ИТОГО: O(n + m log m) вместо O(n*m + m²)" << std::endl;
    
    
    freeFreqArray(freq_array, unique_tokens);
    delete[] freq_dist;
    
    std::cout << "\n=== ТОКЕНИЗАЦИЯ ЗАВЕРШЕНА ===" << std::endl;
    
    return 0;
}
