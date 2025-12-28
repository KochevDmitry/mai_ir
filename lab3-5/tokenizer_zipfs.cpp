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
}
const int HASH_TABLE_SIZE = 50021;

unsigned int hashFunction(const char* str) {
    unsigned int hash = 5381;
    int c;
    
    while ((c = *str++)) {
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

double calculateZipfConstant(FreqPair* freq_array, int unique_tokens, int total_tokens) {
    
    
    double harmonic = 0;
    int limit = unique_tokens;
    
    for (int i = 1; i <= limit; i++) {
        harmonic += 1.0 / i;
    }
    
    return total_tokens / harmonic;
}


double zipfPrediction(int rank, double C) {
    return C / rank;
}


void analyzeZipfLaw(FreqPair* freq_array, int unique_tokens, int total_tokens) {
    std::cout << "\n9. АНАЛИЗ ЗАКОНА ЦИПФА" << std::endl;
    std::cout << "========================================" << std::endl;
    
    
    double C = calculateZipfConstant(freq_array, unique_tokens, total_tokens);
    
    std::cout << "\nКонстанта Ципфа C = " << C << std::endl;
    
    
    std::cout << "\nСРАВНЕНИЕ РЕАЛЬНЫХ И ПРЕДСКАЗАННЫХ ЧАСТОТ:" << std::endl;
    std::cout << "Ранг\tТокен\t\tРеальная\tЦипф\tОтклонение\tlog(Ранг)\tlog(Частота)" << std::endl;
    std::cout << "---------------------------------------------------------------------------------" << std::endl;
    
    double total_error = 0;
    int compare_count = (unique_tokens < 50) ? unique_tokens : 50;
    
    for (int i = 0; i < compare_count; i++) {
        int rank = i + 1;
        int real_freq = freq_array[i].freq;
        double zipf_freq = zipfPrediction(rank, C);
        double error = ((real_freq - zipf_freq) / zipf_freq) * 100;
        
        double log_rank = myLog((double)rank);
        double log_freq = myLog((double)real_freq);
        
        total_error += (error > 0 ? error : -error); 
        
        std::cout << rank << "\t";
        
        
        int token_len = myStrlen(freq_array[i].text);
        std::cout << freq_array[i].text;
        if (token_len < 8) std::cout << "\t\t";
        else std::cout << "\t";
        
        std::cout << real_freq << "\t\t"
                  << (int)zipf_freq << "\t"
                  << error << "%\t\t"
                  << log_rank << "\t"
                  << log_freq << std::endl;
    }
    
    double avg_error = total_error / compare_count;
    std::cout << "\nСредняя абсолютная ошибка (первые " << compare_count << " слов): " 
              << avg_error << "%" << std::endl;
    
    
    std::cout << "\nОЦЕНКА СООТВЕТСТВИЯ ЗАКОНУ ЦИПФА:" << std::endl;
    if (avg_error < 20) {
        std::cout << "✓ ОТЛИЧНО - распределение хорошо соответствует закону Ципфа" << std::endl;
    } else if (avg_error < 40) {
        std::cout << "~ ХОРОШО - есть некоторые отклонения, но в целом соответствует" << std::endl;
    } else if (avg_error < 60) {
        std::cout << "⚠ УДОВЛЕТВОРИТЕЛЬНО - заметные отклонения от закона Ципфа" << std::endl;
    } else {
        std::cout << "✗ ПЛОХО - распределение значительно отличается от закона Ципфа" << std::endl;
    }
    
    
    FILE* zipf_file = fopen("zipf_analysis.csv", "w");
    if (zipf_file) {
        fprintf(zipf_file, "rank,token,real_freq,zipf_freq,log_rank,log_freq,log_zipf\n");
        
        int save_count = unique_tokens;
        for (int i = 0; i < save_count; i++) {
            int rank = i + 1;
            int real_freq = freq_array[i].freq;
            double zipf_freq = zipfPrediction(rank, C);
            
            fprintf(zipf_file, "%d,%s,%d,%.2f,%.4f,%.4f,%.4f\n",
                    rank,
                    freq_array[i].text,
                    real_freq,
                    zipf_freq,
                    myLog((double)rank),
                    myLog((double)real_freq),
                    myLog(zipf_freq));
        }
        
        fclose(zipf_file);
        std::cout << "\nДанные для графика сохранены в: zipf_analysis.csv" << std::endl;
    }
    
    
    std::cout << "\nВОЗМОЖНЫЕ ПРИЧИНЫ ОТКЛОНЕНИЙ:" << std::endl;
    std::cout << "1. Тематическая специфика корпуса (F1 - узкая тема)" << std::endl;
    std::cout << "2. Много имен собственных (гонщики, команды, трассы)" << std::endl;
    std::cout << "3. Технические термины с высокой частотой" << std::endl;
    std::cout << "4. Ограниченный размер корпуса" << std::endl;
    std::cout << "5. Особенности языка (служебные слова)" << std::endl;
}

bool isDelimiter(char c) {
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
        if (token_len > 0 && token_len < 50) { 
            
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

void freeFreqArray(FreqPair* array, int size) {
    for (int i = 0; i < size; i++) {
        delete[] array[i].text;
    }
    delete[] array;
}

int main() {
    std::cout << "=== ТОКЕНИЗАЦИЯ И ПОДСЧЕТ ЧАСТОТ (ОПТИМИЗИРОВАННАЯ) ===" << std::endl;
    
    
    int file_size;
    char* xml_content = readFile("../lab2/articles.xml", file_size);
    if (!xml_content) {
        return 1;
    }
    
    std::cout << "\n1. ЧТЕНИЕ ФАЙЛА" << std::endl;
    std::cout << "Размер XML файла: " << file_size << " байт" << std::endl;
    
    
    std::cout << "\n2. ИЗВЛЕЧЕНИЕ ТЕКСТА ИЗ CONTENT" << std::endl;
    
    int extracted_size;
    char* text = extractContentFromXML(xml_content, file_size, extracted_size);
    
    delete[] xml_content;
    
    std::cout << "Извлечено текста: " << extracted_size << " байт" << std::endl;
    
    
    if (extracted_size == 0) {
        std::cerr << "Ошибка: не удалось извлечь текст из XML!" << std::endl;
        
        
        std::cout << "Первые 500 символов XML:" << std::endl;
        for (int i = 0; i < 500 && i < file_size; i++) {
            std::cout << xml_content[i];
        }
        std::cout << std::endl;
        
        return 1;
    }
    
    
    std::cout << "\n3. ТОКЕНИЗАЦИЯ С ФИЛЬТРАЦИЕЙ" << std::endl;
    
    HashMap hashmap;
    int total_tokens;
    
    tokenizeWithHashMap(text, hashmap, total_tokens);
    
    delete[] text;
    
    int unique_tokens = hashmap.getUniqueCount();
    
    std::cout << "Всего токенов (после фильтрации): " << total_tokens << std::endl;
    std::cout << "Уникальных токенов: " << unique_tokens << std::endl;
    
    
    FreqPair* freq_array = hashmap.toArray();
    sortFreqArray(freq_array, unique_tokens);
    
    
    std::cout << "\n4. АНАЛИЗ ЗАКОНА ЦИПФА" << std::endl;
    
    
    
    double harmonic = 0;
    for (int i = 1; i <= unique_tokens; i++) {
        harmonic += 1.0 / i;
    }
    double C = total_tokens / harmonic;
    
    std::cout << "Константа Ципфа C = " << (int)C << std::endl;
    
    
    std::cout << "\nСРАВНЕНИЕ РЕАЛЬНЫХ И ПРЕДСКАЗАННЫХ ЧАСТОТ:" << std::endl;
    std::cout << "Ранг\tТокен\t\t\tРеальная\tЦипф\tОтклонение\tlog(Ранг)\tlog(Частота)" << std::endl;
    std::cout << "---------------------------------------------------------------------------------" << std::endl;
    
    double total_deviation = 0;
    int compare_count = (unique_tokens < 30) ? unique_tokens : 30;
    
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
    
    std::cout << "\nСредняя абсолютная ошибка (первые " << compare_count << " слов): " 
              << total_deviation / compare_count << "%" << std::endl;
    
    
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
    
    
    std::cout << "\n6. ОЦЕНКА КАЧЕСТВА ЗАКОНА ЦИПФА" << std::endl;
    
    if (total_deviation / compare_count < 30) {
        std::cout << "✓ Распределение ХОРОШО соответствует закону Ципфа" << std::endl;
        std::cout << "  Среднее отклонение: " << total_deviation / compare_count << "% (< 30% - хорошо)" << std::endl;
    } else if (total_deviation / compare_count < 50) {
        std::cout << "~ Распределение УДОВЛЕТВОРИТЕЛЬНО соответствует закону Ципфа" << std::endl;
        std::cout << "  Среднее отклонение: " << total_deviation / compare_count << "% (30-50% - удовлетворительно)" << std::endl;
    } else {
        std::cout << "✗ Распределение ПЛОХО соответствует закону Ципфа" << std::endl;
        std::cout << "  Среднее отклонение: " << total_deviation / compare_count << "% (> 50% - плохо)" << std::endl;
        std::cout << "  Возможные причины:" << std::endl;
        std::cout << "  - Текст слишком мал для формирования статистики" << std::endl;
        std::cout << "  - Специфичная тематика (F1) с большим количеством имен собственных" << std::endl;
        std::cout << "  - Недостаточная очистка от технических токенов" << std::endl;
    }
    
    
    saveTokenList(freq_array, unique_tokens, "tokens_list.txt");
    
    
    freeFreqArray(freq_array, unique_tokens);
    
    std::cout << "\n=== АНАЛИЗ ЗАВЕРШЕН ===" << std::endl;
    
    return 0;
}
