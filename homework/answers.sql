### Pешение первой домашней работы sql
В качесте СУБД был использован postgresql. Я напоминаю здесь что здаю с опаздание и при проверке не забудьте про понижающий коэффициент 0.5;

Ниже будет подбробно описаны предпосылки которые я использовал.

1. В задании я принял следующие предпосылки; 
- Первый id-шник это id департамента, равный id самого сотрудника.
- Второе число это номер начальника = номеру департамента в котором находится сотрудник
- Тоесть любой сотрудник кроме первого находится в двух состояних: он глава какого-то отдела, если у него есть подчиненные, а также сотрудник другого.
