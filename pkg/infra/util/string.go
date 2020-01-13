package util

func RemoveDuplicate(source []string) (result []string){
	if len(source) == 0 {
		return
	}
	tmpMap := make(map[string]interface{})
	for _, s := range source {
		if _, ok := tmpMap[s]; !ok {
			result = append(result, s)
			tmpMap[s] = nil
		}
	}
	return
}
